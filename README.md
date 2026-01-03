# Project for ID2223

This project aims to predict the water temperature at Bathing sits at Södertäljje kommun, using their open water temperature data (https://www.dataportal.se/datasets/75_7058 and https://www.dataportal.se/datasets/75_7659) and weather data from openmeteo (https://open-meteo.com/). It is a batch ML system making daily predictions of water temperature. The predictions can be seen in the UI linked below.

Link to UI: [Result Dashboard](https://isabell257.github.io/id2223-project/water-temp/)

## Feature pipeline
The feature pipeline is divided into two notebooks, one for feature backfill mode and one that is run daily using github actions to update the data with today's water temperature and weather data for today and forecast for predictions. The features are stored in two seperate feature groups, one with water temperature data and one with weather data. Lagged features are created as a model-independent transformations and data is checked for missing values and validated using Great Expectations with Hopsworks feature store.

## Training pipeline
The training pipeline utilizes feature data from the feature store to train a model. A feature view is created to define the model schema and generate train and test data. Different models and features were experimented with during development. Both an XGBoost and a CatBoost model tested. The water temperature is the label and features used for experiments were both lagged water temperature up to three days back and the weather features temperature, precipitation, wind speed, wind direction, shortwave solar radiation and solar radiation. The bath location where the water temperature is measured is also used as a feature, which is encoded as a categorical feature.

However, the best result was an MSE of 0,523 from a CatBoost model using all features mentioned above and is therefore the model that is trained in the training pipeline.

## Inference pipeline
The batch inference pipeline is run once per day with github actions, using the weather forecast and predicted lagged features. The trained model is downloaded from the model registry and the same model-dependent transformation is performed on the bath location to prevent a skew. Predictions are plotted in a graph and logged in the feature store in a seperate feature group. Those are used to create a hindsight graph, comparing the prediction with the actual measurements.

## UI
The github pages UI displays both hindsight and prediction graphs for each bath location. Each bath location has its own page and navigation is done via a dropdown menu.






