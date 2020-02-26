import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from sklearn.metrics import accuracy_score, hamming_loss, fbeta_score


class BiGRU(nn.Module):
    """BiDirectional GRU neural network model.

    Parameters
    ----------
    hidden_size: int
        Number of features in the hidden state.
    n_features: int
        Number of the input features.
    output_size: int
        Number of classes.
    n_layers: int, optional (default=1)
        Number of stacked recurrent layers.
    clip: int, optional (default=50)
        Max norm of the gradients.
    dropout: float, optional (default=0.2)
        Probability of an element of the tensor to be zeroed.
    spatial_dropout: boolean, optional (default=True)
        Whether to use the spatial dropout.
    bidirectional: boolean, optional (default=True)
        Whether to use the bidirectional GRU.

    """

    def __init__(self, hidden_size, n_features, output_size, n_layers=1, clip=50, dropout=0.2,
                 spatial_dropout=True, bidirectional=True):

        # Inherit everything from the nn.Module
        super(BiGRU, self).__init__()

        # Initialize attributes
        self.hidden_size = hidden_size
        self.n_features = n_features
        self.output_size = output_size
        self.n_layers = n_layers
        self.clip = clip
        self.dropout_p = dropout
        self.spatial_dropout = spatial_dropout
        self.bidirectional = bidirectional
        self.n_directions = 2 if self.bidirectional else 1

        # Initialize layers
        self.dropout = nn.Dropout(self.dropout_p)
        if self.spatial_dropout:
            self.spatial_dropout1d = nn.Dropout2d(self.dropout_p)

        self.gru = nn.GRU(self.n_features, self.hidden_size, num_layers=self.n_layers,
                          dropout=(0 if n_layers == 1 else self.dropout_p), batch_first=True,
                          bidirectional=self.bidirectional)

        # Linear layer input size is equal to hidden_size * 3, becuase
        # we will concatenate max_pooling ,avg_pooling and last hidden state
        self.linear = nn.Linear(self.hidden_size * 3, self.output_size)


    def forward(self, input_seq, hidden=None):
        """Forward propagates through the neural network model.

        Parameters
        ----------
        input_seq: torch.Tensor
            Batch of input sequences.
        hidden: torch.FloatTensor, optional (default=None)
            Tensor containing initial hidden state.

        Returns
        -------
        torch.Tensor
            Logits - vector of raw predictions in range (-inf, inf). Probability of 0.5 corresponds
            to a logit of 0. Positive logits correspond to probabilities greater than 0.5, while negative
            to probabilities less than 0.5.

        """
        # Extract batch_size
        self.batch_size = input_seq.size(0)

        # Sequence length
        self.input_length = input_seq.size(1)

        if self.spatial_dropout:
            # Convert to (batch_size, n_features, seq_length)
            input_seq = input_seq.permute(0, 2, 1)
            input_seq = self.spatial_dropout1d(input_seq)
            # Convert back to (batch_size, seq_length, n_features)
            input_seq = input_seq.permute(0, 2, 1)
        else:
            input_seq = self.dropout(input_seq)

        # GRU input/output shapes, if batch_first=True
        # Input: (batch_size, seq_len, n_features)
        # Output: (batch_size, seq_len, hidden_size*num_directions)
        # Number of directions = 2 when used bidirectional, otherwise 1
        # shape of hidden: (n_layers x num_directions, batch_size, hidden_size)
        # Hidden state defaults to zero if not provided
        gru_out, hidden = self.gru(input_seq, hidden)
        # gru_out: tensor containing the output features h_t from the last layer of the GRU
        # gru_out comprises all the hidden states in the last layer ("last" depth-wise, not time-wise)
        # For biGRu gru_out is the concatenation of a forward GRU representation and a backward GRU representation
        # hidden (h_n) comprises the hidden states after the last timestep

        # Extract and sum last hidden state
        # Input hidden shape: (n_layers x num_directions, batch_size, hidden_size)
        # Separate hidden state layers
        hidden = hidden.view(self.n_layers, self.n_directions, self.batch_size, self.hidden_size)
        last_hidden = hidden[-1]
        # last hidden shape (num_directions, batch_size, hidden_size)
        # Sum the last hidden state of forward and backward layer
        last_hidden = torch.sum(last_hidden, dim=0)
        # Summed last hidden shape (batch_size, hidden_size)

        # Sum the gru_out along the num_directions
        if self.bidirectional:
            gru_out = gru_out[:,:,:self.hidden_size] + gru_out[:,:,self.hidden_size:]

        # Select the maximum value over each dimension of the hidden representation (max pooling)
        # Permute the input tensor to dimensions: (batch_size, hidden, seq_len)
        # Output dimensions: (batch_size, hidden_size)
        max_pool = F.adaptive_max_pool1d(gru_out.permute(0,2,1), (1,)).view(self.batch_size,-1)

        # Consider the average of the representations (mean pooling)
        # Sum along the batch axis and divide by the corresponding length (FloatTensor)
        # Output shape: (batch_size, hidden_size)
        avg_pool = torch.sum(gru_out, dim=1) / torch.FloatTensor([self.input_length])

        # Concatenate max_pooling, avg_pooling and last hidden state tensors
        concat_out = torch.cat([last_hidden, max_pool, avg_pool], dim=1)

        # concat_out = self.dropout(concat_out)
        # output_size: batch_size, output_size
        out = self.linear(concat_out)
        return out


    def add_loss_fn(self, loss_fn):
        """Add loss function to the model.

        """
        self.loss_fn = loss_fn


    def add_optimizer(self, optimizer):
        """Add optimizer to the model.

        """
        self.optimizer = optimizer


    def add_device(self, device=torch.device('cpu')):
        """Specify the device.

        """
        self.device = device


    def train_model(self, train_iterator):
        """Performs single training epoch.

        Parameters
        ----------
        train_iterator: torch.utils.data.dataloader.DataLoader
            Pytorch dataloader of MySQLBatchLoader dataset.

        Returns
        -------
        accuracies: float
            Mean accuracy.
        hamm_losses: float
            Mean Hamming loss.
        losses: float
            Mean loss.
        fbetas_list: list
            Mean fbeta score within each class.

        """
        self.train()

        losses = []
        accuracies = []
        hamm_losses = []
        fbetas_list = []

        for input_seq, target in train_iterator:
            # input_seq size: batch_size, seq_len, n_features
            # target size: batch_size, 1, n_classes
            # Reshape target to size: batch_size, n_classes
            target = target.squeeze(1)

            input_seq.to(self.device)
            target.to(self.device)

            self.optimizer.zero_grad()

            pred = self.forward(input_seq)

            loss = self.loss_fn(pred, target)

            loss.backward()
            losses.append(loss.data.cpu().numpy())

            # Clip gradients: gradients are modified in place
            _ = nn.utils.clip_grad_norm_(self.parameters(), self.clip)

            self.optimizer.step()

            # Map logits to probabilities and extract labels using threshold
            pred = torch.sigmoid(pred) > 0.5

            ac_score = accuracy_score(target, pred)
            accuracies.append(ac_score)

            hamm_loss = hamming_loss(target, pred)
            hamm_losses.append(hamm_loss)

            fbeta = fbeta_score(target, pred, beta=0.5, average=None)
            fbetas_list.append(fbeta)

        return np.mean(accuracies), np.mean(hamm_losses), np.mean(losses), np.mean(fbetas_list, axis=0)


    def evaluate_model(self, eval_iterator):
        """Perform the one evaluation epoch.

        Parameters
        ----------
        eval_iterator: torch.utils.data.dataloader.DataLoader
            Pytorch dataloader of MySQLBatchLoader dataset.

        Returns
        -------
        accuracies: float
            Mean accuracy.
        hamm_losses: float
            Mean Hamming loss.
        fbetas_list: list
            Mean fbeta score within each class.
        pred_total: torch.LongTensor()
            Tensor containing chunk's generated predictions
        target_total: torch.LongTensor()
            Tensor containing chunk's target variables.

        """
        self.eval()

        # losses = []
        accuracies = []
        hamm_losses = []
        fbetas_list = []
        pred_total = torch.LongTensor()
        target_total = torch.LongTensor()

        with torch.no_grad():
            for input_seq, target in eval_iterator:
                target = target.squeeze(1)

                input_seq.to(self.device)
                target.to(self.device)

                pred = self.forward(input_seq)

                # loss = self.loss_fn(pred, target)

                # losses.append(loss.data.cpu().numpy())

                # Map logits to probabilities and extract labels using threshold
                pred = torch.sigmoid(pred) > 0.5

                ac_score = accuracy_score(target, pred)
                accuracies.append(ac_score)

                hamm_loss = hamming_loss(target, pred)
                hamm_losses.append(hamm_loss)

                fbeta = fbeta_score(target, pred, beta=0.5, average=None)
                fbetas_list.append(fbeta)

                pred_total = torch.cat([pred_total, pred.type(torch.LongTensor)], dim=0)
                target_total = torch.cat([target_total, target.type(torch.LongTensor)], dim=0)

        return  np.mean(accuracies), np.mean(hamm_losses), np.mean(fbetas_list, axis=0), pred_total, target_total
