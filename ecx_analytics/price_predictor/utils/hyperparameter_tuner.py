from numpy import mean
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import TimeSeriesSplit
from skopt.space import Integer
from skopt.space import Real
from skopt.space import Categorical
from skopt.utils import use_named_args
from skopt import gp_minimize


class HyperparameterTuner:
    def __init__(self, model, hyperparameter_set, X, y, n_splits=3, test_size=100, n_calls=50):
        self.model = model
        self.hyperparameter_set = hyperparameter_set
        self.X = X
        self.y = y
        self.n_splits = n_splits
        self.test_size = test_size
        self.n_calls = n_calls
        self.search_space = []

    @staticmethod
    def create_search_parameter(param_dict):
        if param_dict['type'] == 'Real':
            lower = param_dict['lower']
            upper = param_dict['upper']
            method = param_dict['method']
            name = param_dict['name']
            return Real(lower, upper, method, name=name)
        elif param_dict['type'] == ['Integer']:
            lower = param_dict['lower']
            upper = param_dict['upper']
            name = param_dict['name']
            return Integer(lower, upper, name=name)
        elif param_dict['type'] == 'Categorical':
            enum = param_dict['enum']
            name = param_dict['name']
            return Categorical(enum, name=name)

    def evaluate_model(self, **params):
        # configure the model with specific hyperparameters
        self.model.set_params(**params)
        # define test harness
        cv = TimeSeriesSplit(n_splits=self.n_splits, test_size=self.test_size)
        # calculate TS cross validation
        result = cross_val_score(self.model, self.X, self.y, cv=cv, n_jobs=-1, scoring='neg_mean_squared_error')
        # calculate the mean of the scores
        estimate = mean(result)
        # convert from a maximizing score to a minimizing score
        return - estimate
        

    def find_params(self):
        param_names = []
        for p in self.hyperparameter_set:
            self.search_space.append(self.create_search_parameter(p))
            param_names.append(p['name'])
        
        @use_named_args(dimensions=self.search_space)
        def fitness_wrapper(**kwargs):
            return self.evaluate_model(**kwargs)
        
        result = gp_minimize(func=fitness_wrapper, dimensions=self.search_space, n_calls=self.n_calls)
        params = dict(zip(param_names, result.x))
        return params
        
 
    