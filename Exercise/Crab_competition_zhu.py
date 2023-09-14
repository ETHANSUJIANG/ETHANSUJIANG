# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 16:50:40 2023
@author: zhusj
"""
import pandas as pd
import numpy as np
import os
import sklearn.preprocessing as skp
import sklearn.metrics as skm
import sklearn.ensemble as sken
from sklearn.model_selection import train_test_split
from sklearn.feature_selection import mutual_info_regression
from sklearn.feature_selection import mutual_info_classif
import torch
#import torchvision
import torch.nn as nn # All neural network modules, nn.Linear, nn.Conv2d, BatchNorm, Loss functions
import torch.optim as optim # For all Optimization algorithms, SGD, Adam, etc.
import torch.nn.functional as F # All functions that don't have any parameters
#from torch.utils.data import DataLoader # Gives easier dataset managment and creates mini batches
import torchvision.datasets as datasets # Has standard datasets we can import in a nice way
import torchvision.transforms as transforms # Transformations we can perform on our dataset

# data prepare
print(os.getcwd())
df = pd.read_csv('data1/crab_train.csv')
val_data = pd.read_csv('data1/crab_test.csv',index_col=False)
print(val_data.head(1))
df0 = df.copy()
df0['Sex'].replace({'F':1,'I':0,'M':-1},inplace=True)
X = df0.iloc[:,1:-1]
X['Crab'] =(X.iloc[:,4]+X.iloc[:,5]+X.iloc[:,6]+X.iloc[:,-1])/(4)
X['Weight'] = (X['Weight']-  X['Weight'].mean())/X['Weight'].std()
# X['We-She'] = X['Shell Weight']+X['Weight']
y = df0.iloc[:,-1]
X_train,X_test, y_train, y_test = train_test_split(X,y,test_size =0.1,random_state= 50)
## mutual information quickly look
feature_name = X.columns
mi_number_re = pd.Series(mutual_info_regression(X,y))
mi_number_re.index = feature_name 
mi_number_cl = pd.Series(mutual_info_classif(X,y))
mi_number_cl.index = feature_name 
print('mi_number_re:',mi_number_re)
print('mi_number_cl:',mi_number_cl)
## convert data to tensor 
print(len(X_train)%1024)
X_train = torch.tensor(np.asarray(X_train), dtype=torch.float32)
y_train = torch.tensor(np.asarray(y_train), dtype=torch.float32)
X_test = torch.tensor(np.asarray(X_test), dtype=torch.float32)
y_test = torch.tensor(np.asarray(y_test), dtype=torch.float32)

# build up network
class NN(nn.Module):
    def __init__(self,input_size,num_classes,hidden_dim1,hidden_dim2):
        super(NN, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(input_size, hidden_dim1),
            nn.ReLU(),
            nn.Linear(hidden_dim1, hidden_dim2),
            nn.ReLU(),
            nn.Linear(hidden_dim2, hidden_dim2),
            nn.ReLU(),
            nn.Linear(hidden_dim2, num_classes),
        )
  
    def forward(self, x):
        x = x.view(x.size(0), -1)
        out = self.model(x)
        return out
device = torch.device("cuda" if torch.cuda.is_available() else 'cpu')# set device
## initial parameter
input_size = 9
num_classes = 1
## Hyperparameter
config = {'batch_size':128,
          'hidden_dim1':64,
          'hidden_dim2':128,
          'learning_rate':0.00135,
          'num_epochs':10}

model = NN(input_size = input_size,num_classes = num_classes,
           hidden_dim1=config['hidden_dim1'],hidden_dim2=config['hidden_dim2']).to(device)

# define the loss

criterion = nn.MSELoss()

#optimizer

optimizer = optim.Adam(model.parameters(),lr = config['learning_rate'])


#training Network

for epoch in range(config['num_epochs']):
    # forward
    #num = len(X_train)//config['batch_size']
    #rest = len(X_train)%config['batch_size']
    
    for num in range(len(X_train)//config['batch_size']):
        bs = config['batch_size']
        X_batch_data= X_train[bs*num:bs*(num+1),:]
        y_batch_data = y_train[bs*num:bs*(num+1)]
        predict = model(X_batch_data.to(device))
        predict = torch.squeeze(predict)
        loss = criterion(predict, y_batch_data.to(device))
        # print("predict:",predict.shape)
        # print("y_train:",y_train.shape)
        
        #backward
        optimizer.zero_grad() # initial parameters
        
        loss.backward() 
        
        # gradient descent or adam step
        optimizer.step()
    
    print(f'iter:{epoch},test loss:{loss}')
  
## use test dataset testing
# model.eval()
# val_loss =0
# with torch.no_grad():
# Test out inference with 5 samples
    predict_test= model(X_test)
    predict_test = torch.squeeze(predict_test)
    val_loss = (abs(predict_test-y_test)).sum()/len(y_test)
    print('val_loss:',val_loss)
    print(predict_test[0:10],y_test[0:10])
val_data.iloc[0,:]
val_index = val_data['id']
val_data = val_data['Sex'].replace({'F':1,'I':0,'M':-1},inplace=True)
val_data['Crab'] =(val_data['Weight']+val_data['Shucked Weight']+val_data['Viscera Weight']+val_data['Viscera Weight'])/(4)
val_data['Weight'] = (val_data['Weight']-  val_data['Weight'].mean())/val_data['Weight'].std()
val_data = torch.tensor(np.asarray(val_data), dtype=torch.float32)
val_predict = model(val_data)
val_predict = np.asarray(val_predict)
val_predict = pd.DataFrame(val_predict)
val_predict.index = val_index
val_predict.to_csv('data1/crab_yield.csv')
# Check accuracy on training & test to see how good our model

