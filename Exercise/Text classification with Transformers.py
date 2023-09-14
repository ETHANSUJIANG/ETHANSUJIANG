#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os

import numpy as np
import gc
import pandas as pd
import tensorflow as tf
from tensorflow.keras.layers import Dense, Input
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.models import Model
from tensorflow.keras.callbacks import ModelCheckpoint
import transformers
from transformers import TFAutoModel, AutoTokenizer
from tqdm.notebook import tqdm
import numpy as np
from keras.utils import np_utils
from sklearn.preprocessing import LabelEncoder

from sklearn.metrics import f1_score
from tensorflow.keras.callbacks import Callback 
from tokenizers import Tokenizer, models, pre_tokenizers, decoders, processors

strategy = tf.distribute.get_strategy()


# In[ ]:


def regular_encode(texts, tokenizer, maxlen=512):
    """
    encodes text for a model
    """
    enc_di = tokenizer.batch_encode_plus(
        texts,
        return_token_type_ids=False,
        pad_to_max_length=True,
        max_length=maxlen
    )
    
    return np.array(enc_di['input_ids'])


# In[ ]:


def build_model(transformer, max_len=512, hidden_dim=32, n_classes=1):
    """
    builds a model
    """
    input_word_ids = Input(shape=(max_len,), dtype=tf.int32, name="input_word_ids")
    sequence_output = transformer(input_word_ids)[0]
    cls_token = sequence_output[:, 0, :]
    
    if n_classes == 2: # binary classification
        out = Dense(1, activation='sigmoid')(cls_token)
    else:
        out = Dense(n_classes, activation='sigmoid')(cls_token)
    
    model = Model(inputs=input_word_ids, outputs=out)
    
    if n_classes > 2:
        model.compile(Adam(lr=1e-5), loss='categorical_crossentropy', metrics=['accuracy'])
    else:
        model.compile(Adam(lr=1e-5), loss='binary_crossentropy', metrics=['accuracy'])
    
    return model


# In[ ]:


AUTO = tf.data.experimental.AUTOTUNE

# Configuration
EPOCHS = 2
BATCH_SIZE = 16 * strategy.num_replicas_in_sync
MAX_LEN = 162 #TODO: to set it correctly determine what is the average (or max) token length of your training data 
MODEL = 'bert-base-uncased' # use any appropriate model (e.g. bert-base-cased) from https://huggingface.co/models


# In[ ]:


AUTO = tf.data.experimental.AUTOTUNE

# Configuration
EPOCHS = 2
BATCH_SIZE = 16 * strategy.num_replicas_in_sync
MAX_LEN = 162 #TODO: to set it correctly determine what is the average (or max) token length of your training data 
MODEL = 'bert-base-uncased' # use any appropriate model (e.g. bert-base-cased) from https://huggingface.co/models


# In[ ]:


get_ipython().run_cell_magic('time', '', '\nx_train = regular_encode(train.question.values, tokenizer, maxlen=MAX_LEN)\nx_test = regular_encode(test.question.values, tokenizer, maxlen=MAX_LEN)\n\ny_train = train.predicate.values\ny_test = test.predicate.values\n\n# encode textual labels into corresponding numbers\nencoder = LabelEncoder()\nencoder.fit(y_train)\nencoded_y_train = encoder.transform(y_train) \nencoded_y_test = encoder.transform(y_test)\ndummy_y_train = np_utils.to_categorical(encoded_y_train) # convert integers to dummy variables (i.e. one hot encoded)')


# In[ ]:


train_dataset = (
    tf.data.Dataset
    .from_tensor_slices((x_train, dummy_y_train))
    .repeat()
    .shuffle(2048)
    .batch(BATCH_SIZE)
    .prefetch(AUTO)
)

test_dataset = (
    tf.data.Dataset
    .from_tensor_slices(x_test)
    .batch(BATCH_SIZE)
)


# In[ ]:


get_ipython().run_cell_magic('time', '', 'with strategy.scope():\n    transformer_layer = TFAutoModel.from_pretrained(MODEL)\n    model = build_model(transformer_layer, max_len=MAX_LEN, n_classes=train.predicate.nunique())\nmodel.summary()')


# In[ ]:


n_steps = x_train.shape[0] // BATCH_SIZE # determine number of steps per epoch

train_history = model.fit(
    train_dataset,
    steps_per_epoch=n_steps,
    epochs=EPOCHS
)


# In[ ]:


y_pred = np.argmax(model.predict(test_dataset, verbose=1), axis=1)


# In[ ]:


encoder.inverse_transform(y_pred[:5]) # show actual labels


# In[ ]:


print("F1 Score", f1_score(encoded_y_test, y_pred, average='weighted'))


# In[ ]:





# In[ ]:




