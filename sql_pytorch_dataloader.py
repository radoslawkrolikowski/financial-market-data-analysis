import torch
import pickle
from torch.utils.data import Dataset
from itertools import islice
from config import bid_levels, ask_levels


def window_indices(seq, n=2):
    """Returns a sliding window (of width n) over iterable (seq).

    """
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result


class MySQLChunkLoader(Dataset):
    """Consecutively generates indices of database rows that form chunk of MySQL/MariaDB
    database parameterized by chunk_size (chunking is used to diminish memory usage while parsing).
    Calculates data chunk's normalization parameters (MIN, MAX) and save them to file.
    Normalization parameters then can be used to normalize validation and test sets.

    Example of generating chunk indices:

     DB      Table
    table   chunks
    -----   ------
    1.       1.
    2.       2.
    3.  -->  3.
    4.      ------
    5.       3.
             4.
             5.

    Parameters
    ----------
    cursor: mysql.connector.cursor_cext.CMySQLCursor
        MySQL cursor object used to execute SQL statements.
    table: str
        Name of the SQL table to read from.
    db_x_query: str
        SQL query that selects all columns from database that we want to use as input variables.
        (SELECT col_1, col_2 FROM table_1 t1 JOIN table_2 t2 ON t1.ID = t2.ID;)
    chunk_size: int
        Size of the chunk of data to be read from database.
    window: int
        Length of the sliding window (with a stride of 1).

    Returns
    -------
    chunk_indices[idx]: tuple
        Tuple of database indices that form given chunk.
    norm_params[idx]:  tuple(torch.Tensor)
        Tuple of pytorch Tensors containing normalization parameters of a given chunk.

    """
    def __init__(self, cursor, table, db_x_query, chunk_size, window):

        # Extract number of the rows in database
        cursor.execute("SELECT COUNT(ID) FROM {};".format(table))
        db_length = cursor.fetchone()[0]

        self.num_chunks = db_length // chunk_size
        self.chunk_indices = []

        # Generate chunks indices
        for chunk in range(self.num_chunks + 1):
            if chunk == 0:
                self.chunk_indices.append(range(window, chunk_size))
            elif chunk < (db_length // chunk_size):
                self.chunk_indices.append(range(chunk_size * chunk - window + 1, chunk_size * (chunk + 1)))
            else:
                self.chunk_indices.append(range(chunk_size * chunk - window + 1, db_length + 1))

        # Extract x_fields from db_x_query
        db_x_query = [w.strip(",") for w in db_x_query.split()]
        fields_start_idx = db_x_query.index("SELECT")
        fields_end_idx = db_x_query.index("FROM")
        self.x_fields = db_x_query[fields_start_idx + 1: fields_end_idx]

        # Extract FROM table statement
        from_start_idx = db_x_query.index("FROM")
        from_statement = " ".join(db_x_query[from_start_idx:]).strip(";")

        # Calculate chunk's MIN and MAX values used in data normalization
        self.norm_params = []

        x_min_fields = "".join(["MIN({}), ".format(i) for i in self.x_fields]).strip(", ")
        x_max_fields = x_min_fields.replace("MIN", "MAX")

        for chunk in range(self.num_chunks + 1):
            cursor.execute("SELECT {} {} WHERE ID IN {};"\
               .format(x_min_fields, from_statement, tuple(self.chunk_indices[chunk])))

            x_min = torch.Tensor(cursor.fetchall())

            cursor.execute("SELECT {} {} WHERE ID IN {};"\
               .format(x_max_fields, from_statement, tuple(self.chunk_indices[chunk])))

            x_max = torch.Tensor(cursor.fetchall())

            # In order to perform normalization we have to ensure that MIN != MAX
            for i, _ in enumerate(zip(x_min[0], x_max[0])):
                if x_min[0][i] == x_max[0][i]:
                    if x_max[0][i] != 0:
                        x_max[0][i] += x_max[0][i] * 0.001
                    else:
                        x_max[0][i] += 0.001

            self.norm_params.append((x_min, x_max))

        # Extract MIN and MAX with respect to all order book levels
        # Then assign these values to be MIN and MAX that represent entire book in given chunk
        if "sd.bid_0_size" in self.x_fields:

            ask = ["sd.ask_{}_size".format(i) for i in range(ask_levels)]
            bid = ["sd.bid_{}_size".format(i) for i in range(bid_levels)]

            ask_idx = []
            for i in ask:
                try:
                    ask_idx.append(self.x_fields.index(i))
                except ValueError:
                    continue

            bid_idx = []
            for i in bid:
                try:
                    bid_idx.append(self.x_fields.index(i))
                except ValueError:
                    continue

            for x_min, x_max in self.norm_params:
                if ask_idx:
                    x_min[0][ask_idx] = min(x_min[0][ask_idx])
                    x_max[0][ask_idx] = max(x_max[0][ask_idx])
                if bid_idx:
                    x_min[0][bid_idx] = min(x_min[0][bid_idx])
                    x_max[0][bid_idx] = max(x_max[0][bid_idx])

        # Save last chunk's normalization parameters to file
        params_dict = {}

        for i, name in enumerate(self.x_fields):
            params_dict[name] = {"MIN": self.norm_params[-1][0][0][i], "MAX": self.norm_params[-1][1][0][i]}

        with open("norm_params", "wb") as file:
            pickle.dump(params_dict, file)

    def __getitem__(self, idx):
        return tuple(self.chunk_indices[idx]), self.norm_params[idx]

    def __len__(self):
        return self.num_chunks + 1


class MySQLBatchLoader(Dataset):
    """Consecutively generates indices of database rows in a sliding window manner to be read from
    chunk of a MySQL/MariaDB database.

    Example of generating sliding windows within chunks of data:

     DB      Table      Sliding
    table   chunks      windows
    -----   ------      ------
    1.       1.         1., 2.
    2.       2.     --> ------
    3.  -->  3.         2., 3.
    4.      ------      ------
    5.       3.         3., 4.
             4.     --> ------
             5.         4., 5.

    Parameters
    ----------
    indices: range
        Range of database indices that form given chunk.
    norm_params: tuple(torch.Tensor)
        Tuple of pytorch Tensors containing normalization parameters of a given chunk.
    cursor: mysql.connector.cursor_cext.CMySQLCursor
        MySQL cursor object used to execute SQL statements.
    table: str
        Name of the SQL table to read from.
    db_x_query: str
        SQL query that selects all columns from database that we want to use as input variables.
        (SELECT col_1, col_2 FROM table_1 t1 JOIN table_2 t2 ON t1.ID = t2.ID;)
    y_fields: str
        String of database column names separated by commas that will be used as target variables.
        for example: "up1, up2, down1, down2"
    window: int
        Length of the sliding window (with a stride of 1).

    Returns
    -------
    x[[indices]]: torch.Tensor
        Tensor of generated sliding windows of input varaibales.
    y[[indices[-1]]]: torch.Tensor
        Tensor of target variables. Sliding window target corresponds to the right answer of last data point
        of that window. For example for window [5.,6.,7.,8.,9.] the target is [9.] (target variable with id of 9),
        for [6.,7.,8.,9.,10.] target is [10.] and so on.

    """
    def __init__(self, indices, norm_params, cursor, table, db_x_query, y_fields, window):

        super(MySQLBatchLoader, self).__init__()

        indices = tuple(indices)

        # Extract x_fields from db_query
        db_x_query = [w.strip(",") for w in db_x_query.split()]
        fields_start_idx = db_x_query.index("SELECT")
        fields_end_idx = db_x_query.index("FROM")
        x_fields = db_x_query[fields_start_idx + 1: fields_end_idx]
        x_fields_not_null = ", ".join("IFNULL({}, 0)".format(field) for field in x_fields).strip(", ")

        # Extract FROM table statement
        from_start_idx = db_x_query.index("FROM")
        from_statement = " ".join(db_x_query[from_start_idx:]).strip(";")

        # Fetch X (independent variables) from database
        # To use different set of columns modify SQL db_x_query
        cursor.execute("SELECT {} {} WHERE ID IN {};"\
               .format(x_fields_not_null, from_statement, indices))

        self.x = torch.Tensor(cursor.fetchall())

        # Fetch Y (target variables) from database
        cursor.execute("SELECT {} FROM target WHERE ID IN {};"\
               .format(y_fields, indices))

        self.y = torch.Tensor(cursor.fetchall())

        # Normalize X data according to chunk parameters (MIN, MAX)
        self.x = (self.x - norm_params[0][0])/(norm_params[1][0] - norm_params[0][0])

        self.indices_gen = window_indices(range(len(indices)), window)

    def __getitem__(self, idx):
        indices = next(self.indices_gen)
        return self.x[[indices]], self.y[[indices[-1]]]

    def __len__(self):
        return len(self.x)


class TrainValTestSplit:
    """Performs Train/Validation/Test splitting of a set of data chunks,
    which means that val_size and test_size parameters refere to number of
    chunks (not to total number of data points!)

    For example if we have 800 data poins, chunk size of 50 will give us
    16 chunks. If we use val_size=0.1 and test_size=0.1 then spliting will be
    performed in following manner:
    train_set - will contain 0.8 * 16 chunks = 12 chunks
    val_size - (0.1 * 16) + 1 = 2 chunks
    test_size - (0.1 * 16) + 1 = 2 chunks

    Parameters
    ----------
    dataset: MySQLBatchLoader
        MySQLBatchLoader object
    val_size: float, optional (defaul=0.1)
        Validation set size as a fraction of number of data chunks.
    test_size: float, optional (defaul=0.1)
        Test set size as a fraction of number of data chunks.

    Returns
    -------
    training set, validation set, test set that each comprise of:
            chunk_indices[idx]: range
                Range of database indices that form given chunk.
            norm_params[idx]:  tuple(torch.Tensor)
                Tuple of pytorch Tensors containing normalization parameters of a given chunk.

    Raises
    ------
    AssertionError
        If the val_size and test_size sum is greater or equal 1 or the negative value was passed.

    """

    def __init__(self, dataset, val_size=0.1, test_size=0.1):

        assert (val_size + test_size) < 1, 'Validation size and test size sum is greater or equal 1'
        assert val_size >= 0 and test_size >= 0, 'Negative size is not accepted'

        self.dataset = dataset
        self.train_size = 1 - val_size - test_size
        self.val_size = val_size
        self.test_size = test_size

        self.dataset_len = len(self.dataset)

    def get_train(self):
        self.train_end_idx = int(self.train_size * self.dataset_len)
        train_set_ind = self.dataset[0:self.train_end_idx][0]
        train_set_norms = self.dataset[0:self.train_end_idx][1]
        return zip(train_set_ind, train_set_norms)

    def get_val(self):
        self.val_start_idx = self.train_end_idx
        self.val_end_idx = self.val_start_idx + int(self.val_size * self.dataset_len) + 1
        val_set_ind = self.dataset[self.val_start_idx:self.val_end_idx][0]
        val_set_norms = self.dataset[self.val_start_idx:self.val_end_idx][1]
        return zip(val_set_ind, val_set_norms)

    def get_test(self):
        self.test_start_idx = self.val_end_idx
        self.test_end_idx = self.test_start_idx + int(self.test_size * self.dataset_len) + 1
        test_set_ind = self.dataset[self.test_start_idx:self.test_end_idx][0]
        test_set_norms = self.dataset[self.test_start_idx:self.test_end_idx][1]
        return zip(test_set_ind, test_set_norms)

    def get_sets(self):
        return self.get_train(), self.get_val(), self.get_test()
