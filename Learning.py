from DataBase import DB
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
import joblib
from sklearn.model_selection import train_test_split
from sklearn import clone
from sklearn import XGBClassifier, RandomForestClassifier

class Learning():
    
    def __init__(self):
        self.db = DB()
        self.models = {
            'RandomForest': RandomForestRegressor(
            n_estimators=200,
            max_depth=10,
            random_state=42,
            n_jobs=-1
            ),

            'GradientBoosting': GradientBoostingRegressor(
                n_estimators=200,
                learning_rate=0.05,
                max_depth=5,
                random_state=42
            ),

            'XGBoost': XGBRegressor(
                n_estimators=300,
                learning_rate=0.03,
                max_depth=8,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42
            )
        }
        
    def Preprocessing(self):
        dam_data_dict_DataFrame, dam_code_list_int = self.db.Load_ALL_Data()
        all_dam_data_dict_df = {}
        
        for code in dam_code_list_int:
            df = dam_data_dict_DataFrame.get(code)
            df.columns = ['dam_code', 'inflowqy', 'lowlevel', 'obsrdt', 'rf', 'rsvwtqy', 
                            'rsvwtrt', 'totdcwtrqy', 'tmp', 'rain', 'snow']
            df = df.dropna(axis=1, how='all')
            df['rain'] = df['rain'].fillna(0.0)
            df['obsrdt'] = pd.to_datetime(df['obsrdt'])

            df = df.sort_values('obsrdt').reset_index(drop=True)

            # 결측 처리
            df = df.copy()
            # 시간 정리
            df['obsrdt'] = pd.to_datetime(df['obsrdt'])
            df = df.sort_values('obsrdt').reset_index(drop=True)

            # 기본 결측 처리
            df['rain'] = df['rain'].fillna(0)

            fill_cols = ['inflowqy', 'lowlevel', 'rsvwtqy', 'rsvwtrt', 'totdcwtrqy']
            df[fill_cols] = df[fill_cols].ffill().bfill()

            # 누적 강우량
            df['rain_1h'] = df['rain']
            df['rain_3h'] = df['rain'].rolling(3).sum()
            df['rain_6h'] = df['rain'].rolling(6).sum()
            df['rain_12h'] = df['rain'].rolling(12).sum()
            df['rain_24h'] = df['rain'].rolling(24).sum()

            # 유입량 lag
            df['inflow_prev1'] = df['inflowqy'].shift(1)
            df['inflow_prev3'] = df['inflowqy'].shift(3)
            df['inflow_prev6'] = df['inflowqy'].shift(6)

            # 방류량 lag
            df['outflow_prev1'] = df['totdcwtrqy'].shift(1)
            df['outflow_prev3'] = df['totdcwtrqy'].shift(3)
            df['outflow_prev6'] = df['totdcwtrqy'].shift(6)

            # 수위 / 저수량 lag
            df['waterlevel_prev1'] = df['lowlevel'].shift(1)
            df['waterlevel_prev3'] = df['lowlevel'].shift(3)
            df['storage_prev1'] = df['rsvwtqy'].shift(1)

            # 변화량
            df['inflow_diff_1h'] = df['inflowqy'].diff(1)
            df['water_diff_1h'] = df['lowlevel'].diff(1)
            df['outflow_diff_1h'] = df['totdcwtrqy'].diff(1)

            # 시간 변수
            df['month'] = df['obsrdt'].dt.month
            df['hour'] = df['obsrdt'].dt.hour
            df['is_flood_season'] = df['month'].isin([6, 7, 8, 9]).astype(int)

            # 타겟
            df['target_outflow_1h'] = df['totdcwtrqy'].shift(-1) # 1시간뒤 방류량
            df['target_inflow_1h'] = df['inflowqy'].shift(-1) # 1시간뒤 유입량

            df = df.dropna().reset_index(drop=True)

            all_dam_data_dict_df[code] = df

        return all_dam_data_dict_df, dam_code_list_int
    
    # 학습 시작
    def Machine_Learning(self):
        dam_data_dict_df, dam_code_list_int = self.Preprocessing()

        inflow_features = [
            'rain', 'rain_3h', 'rain_6h', 'rain_12h', 'rain_24h',
            'inflowqy', 'inflow_prev1', 'inflow_3h', 'inflow_6h',
            'lowlevel', 'waterlevel_prev1',
            'rsvwtqy', 'storage_prev1',
            'rsvwtrt',
            'totdcwtrqy', 'outflow_prev1',
            'month', 'hour', 'is_flood_season'
        ]

        outflow_features = inflow_features + ['pred_inflow_1h']

        result_list = []

        for dam_code in dam_code_list_int:
            dam_data = dam_data_dict_df.get(dam_code).copy()

            dam_data['inflow_3h'] = dam_data['inflowqy'].rolling(3).sum()
            dam_data['inflow_6h'] = dam_data['inflowqy'].rolling(6).sum()
            dam_data['is_flood_season'] = dam_data['month'].isin([6, 7, 8, 9]).astype(int)

            dam_data = dam_data.dropna().reset_index(drop=True)

            split_idx = int(len(dam_data) * 0.8)

            train_df = dam_data.iloc[:split_idx].copy()
            test_df = dam_data.iloc[split_idx:].copy()

            for model_name, model in self.models.items():

                # 1. 유입량 예측 모델
                inflow_model = clone(model)

                X_train_inflow = train_df[inflow_features]
                y_train_inflow = train_df['target_inflow_1h']

                X_test_inflow = test_df[inflow_features]
                y_test_inflow = test_df['target_inflow_1h']

                inflow_model.fit(X_train_inflow, y_train_inflow)
                pred_inflow = inflow_model.predict(X_test_inflow)

                # 2. 예측 유입량을 test_df에 추가
                test_df_model = test_df.copy()
                test_df_model['pred_inflow_1h'] = pred_inflow

                train_df_model = train_df.copy()
                train_df_model['pred_inflow_1h'] = train_df_model['target_inflow_1h']

                # 3. 방류량 예측 모델
                outflow_model = clone(model)

                X_train_outflow = train_df_model[outflow_features]
                y_train_outflow = train_df_model['target_outflow_1h']

                X_test_outflow = test_df_model[outflow_features]
                y_test_outflow = test_df_model['target_outflow_1h']

                outflow_model.fit(X_train_outflow, y_train_outflow)
                pred_outflow = outflow_model.predict(X_test_outflow)

                mae = mean_absolute_error(y_test_outflow, pred_outflow)
                rmse = np.sqrt(mean_squared_error(y_test_outflow, pred_outflow))
                r2 = r2_score(y_test_outflow, pred_outflow)

                result_list.append({
                    'dam_code': dam_code,
                    'model': model_name,
                    'mae': mae,
                    'rmse': rmse,
                    'r2': r2
                })

                print(f"댐 코드: {dam_code} / 모델: {model_name}")
                print(f"MAE: {mae:.4f}")
                print(f"RMSE: {rmse:.4f}")
                print(f"R2: {r2:.4f}")
                print("=" * 50)

                joblib.dump(inflow_model, f"{model_name}_inflow_{dam_code}.pkl")
                joblib.dump(outflow_model, f"{model_name}_outflow_{dam_code}.pkl")
                
            break

        results = pd.DataFrame(result_list)
        print(results)
        # 모델 성능 저장 함수 호출
        self.db.Model_Performance_Save(results)
        
        bad_models = results[
            (results['r2'] < 0.5) |
            (results['rmse'] > 30)
        ]

        print("성능 안 좋은 모델")
        print(bad_models)
        
        
        
#region 2026년 0514 작업해야함

            
if __name__ == "__main__":
    l = Learning()
    l.Machine_Learning()
            
        
        
        
        
            
            