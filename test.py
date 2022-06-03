from data_validation.train_data_validation.train_validation import TrainValidation
from model_training.training import TrainModel
def train():
    path = 'data'
    train_valObj = TrainValidation(path) #object initialization
    train_valObj.validation()#calling the training_validation function

    train_model = TrainModel()
    train_model.model_training()
train()