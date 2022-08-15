import pandas as pd
import os
import numpy as np

from sqlalchemy import create_engine, text

user = 'habrpguser'
password = 'pgpwd'
host = 'localhost'
port = 5432
database = 'BetterMeTest'


def get_connection(u, pwd, h, p, d):
    return create_engine(
        url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            u, pwd, h, p, d
        )
    )


if __name__ == '__main__':
    load_data = False
    try:
        # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
        engine = get_connection(user, password, host, port, database)
        print(
            f"Connection to the {host} for user {user} created successfully.")
        if pd.read_sql('''SELECT COUNT(DISTINCT TABLE_NAME) AS anyAliasName
                          FROM INFORMATION_SCHEMA.COLUMNS
                          WHERE table_catalog = 'BetterMeTest' and 
                                table_schema = 'public'; ''', engine).iloc[0, 0] == 0:
            with engine.connect() as connection:
                result = connection.execute(text('''
                                                CREATE TABLE public.APP_NAMES (APP_ID SERIAL PRIMARY KEY, APP_NAME VARCHAR(100));
                                                CREATE TABLE public.SUBSCRIPTION_NAMES (SUBSCRIPTION_ID SERIAL PRIMARY KEY, SUBSCRIPTION_NAME VARCHAR(100));
                                                CREATE TABLE public.SUBSCRIPTION_DURATIONS (DURATION_ID SERIAL PRIMARY KEY, SUBSCRIPTION_DURATION VARCHAR(100));
                                                CREATE TABLE public.Currencies (CURRENCY_ID SERIAL PRIMARY KEY, CURRENCY VARCHAR(100));
                                                CREATE TABLE public.DEVICES (DEVICE_ID SERIAL PRIMARY KEY, DEVICE VARCHAR(100));
                                                CREATE TABLE public.COUNTRIES (COUNTRY_ID SERIAL PRIMARY KEY, COUNTRY VARCHAR(100));
                                                CREATE TABLE public.BOOLEAN_VALUES (REFUND_ID SERIAL PRIMARY KEY, REFUND VARCHAR(10));
                                                
                                                CREATE TABLE public.APPLE_REPORTS (REPORT_ID SERIAL PRIMARY KEY, EVENT_DATE DATE,
                                                                      APP_ID SERIAL, APP_APPLE_ID SERIAL, 
                                                                      CONSTRAINT app_id_fk FOREIGN KEY(APP_ID) REFERENCES public.APP_NAMES(APP_ID),
                                                                      SUBSCRIPTION_ID SERIAL, SUBSCRIPTION_APPLE_ID SERIAL, 
                                                                      CONSTRAINT SUBSCRIPTION_ID_fk FOREIGN KEY(SUBSCRIPTION_ID) REFERENCES public.SUBSCRIPTION_NAMES(SUBSCRIPTION_ID), 
                                                                      SUBSCRIPTION_GROUP_ID SERIAL, DURATION_ID SERIAL, INTRODUCTORY_PRICE_TYPE VARCHAR(100), 
                                                                      CONSTRAINT DURATION_ID_fk FOREIGN KEY(DURATION_ID) REFERENCES public.SUBSCRIPTION_DURATIONS(DURATION_ID), 
                                                                      INTRODUCTORY_PRICE_DURATION VARCHAR(100), Marketing_Opt_In_Duration FLOAT(8), 
                                                                      Customer_Price FLOAT(8), Customer_Currency_ID SERIAL, Developer_Proceeds VARCHAR(100), 
                                                                      Proceeds_Currency_ID SERIAL, CONSTRAINT Customer_Currency_ID_fk FOREIGN KEY(Customer_Currency_ID) REFERENCES public.Currencies(Currency_ID), 
                                                                      CONSTRAINT Proceeds_Currency_ID_fk FOREIGN KEY(Proceeds_Currency_ID) REFERENCES public.Currencies(Currency_ID), Preserved_Pricing VARCHAR(100),
                                                                       Proceeds_Reason VARCHAR(100), Client VARCHAR(100), Device_ID SERIAL, 
                                                                       CONSTRAINT DEVICE_ID_fk FOREIGN KEY(DEVICE_ID) REFERENCES DEVICES(DEVICE_ID), 
                                                                       COUNTRY_ID SERIAL, CONSTRAINT COUNTRY_ID_fk FOREIGN KEY(COUNTRY_ID) REFERENCES COUNTRIES(COUNTRY_ID),
                                                                        Subscriber_ID VARCHAR(100), Subscriber_ID_RESET VARCHAR(100), REFUND_ID SERIAL, 
                                                                        CONSTRAINT REFUND_ID_fk FOREIGN KEY(REFUND_ID) REFERENCES BOOLEAN_VALUES(REFUND_ID),
                                                                        PURCHASE_DATE DATE, UNITS SERIAL)
                                                '''))
        else:
            print('----------------------------Знайти тривалість підписки з заданим <id>----------------------------')
            print(pd.read_sql('''SELECT SUBSCRIPTION_DURATIONS.SUBSCRIPTION_DURATION
                                 FROM APPLE_REPORTS APPLE_REPORTS
                                 LEFT JOIN SUBSCRIPTION_DURATIONS SUBSCRIPTION_DURATIONS
                                    ON APPLE_REPORTS.DURATION_ID = SUBSCRIPTION_DURATIONS.DURATION_ID
                                    WHERE APPLE_REPORTS.SUBSCRIBER_ID = '679227409994408'
                                    ''', engine))
            print('----------------------Знайти список підписок для додатку з заданим <id>----------------------')
            print(pd.read_sql('''select apps.app_name
                                 from apple_reports reports
                                 left join app_names apps
                                     on reports.app_id = apps.app_id
                                 where subscriber_id = '588329649050341' ''', engine))
            print('----------------------Знайти список підписок для додатку з заданим <id>----------------------')
            print(pd.read_sql('''select apps.app_name, sum(cast(developer_proceeds as float(10))) as revenue
                                 from apple_reports reports
                                 left join app_names apps
                                     on reports.app_id = apps.app_id
                                 where event_date between '20190204' and '20190206'
                                 group by apps.app_name
                                 order by sum(cast(developer_proceeds as float(10)))
                                 limit 1''', engine))
            print(
                'Знайти конверсію з оформлення пробного періоду в успішний платіж для користувачів, що зробили підписку <id> в дату <date>')
            print(pd.read_sql('''with cte as (select subscriber_id, count(subscriber_id) as cnt_subs
                                 from apple_reports reports
                                 left join SUBSCRIPTION_NAMES sub_names
                                     on reports.subscription_id = sub_names.subscription_id
                                 where subscriber_id in (select subscriber_id from apple_reports reports where Introductory_Price_Type = 'Free Trial' and App_Apple_ID = '1398851503' and Event_Date between '20190204' and '20190206' )
                                 group by subscriber_id
                                 ) select (select count(*) from cte where cnt_subs > 1)::decimal /  count(*) * 100 as conversion_percent
                                 from cte''', engine))
            print('----------------------------------------Кінець----------------------------------------')

        if load_data:
            with engine.connect() as connection:
                filenames = os.listdir('../reports/')
                data = pd.read_csv(f'../reports/{filenames[0]}', sep='\t')
                for i in range(len(filenames)):
                    if i == 0:
                        continue
                    else:
                        data = pd.concat([data, pd.read_csv(f'../reports/{filenames[i]}', sep='\t')])

                for app_name in data['App Name'].unique():
                    connection.execute(text(f'''INSERT INTO public.APP_NAMES(APP_NAME) VALUES ('{app_name}')'''))

                connection.execute(text(f'''INSERT INTO public.APP_NAMES(APP_NAME) VALUES (NULL)'''))

                for sub_name in data['Subscription Name'].unique():
                    connection.execute(
                        text(f'''INSERT INTO public.SUBSCRIPTION_NAMES(SUBSCRIPTION_NAME) VALUES ('{sub_name}')''')
                    )
                connection.execute(
                    text(f'''INSERT INTO public.SUBSCRIPTION_NAMES(SUBSCRIPTION_NAME) VALUES (NULL)''')
                )

                for sub_dur in data['Subscription Duration'].unique():
                    connection.execute(
                        text(
                            f'''INSERT INTO public.SUBSCRIPTION_DURATIONS(SUBSCRIPTION_DURATION) VALUES ('{sub_dur}')''')
                    )

                connection.execute(
                    text(f'''INSERT INTO public.SUBSCRIPTION_DURATIONS(SUBSCRIPTION_DURATION) VALUES (NULL)'''))

                for currency in data['Proceeds Currency'].unique():
                    connection.execute(
                        text(f'''INSERT INTO public.Currencies(Currency) VALUES ('{currency}')'''))
                connection.execute(
                    text(f'''INSERT INTO public.Currencies(Currency) VALUES (NULL)'''))

                for device in data['Device'].unique():
                    connection.execute(
                        text(f'''INSERT INTO public.DEVICES(DEVICE) VALUES ('{device}')'''))
                connection.execute(
                    text(f'''INSERT INTO public.DEVICES(DEVICE) VALUES (NULL)'''))

                for country in data['Country'].unique():
                    connection.execute(
                        text(f'''INSERT INTO public.COUNTRIES(COUNTRY) VALUES ('{country}')'''))

                connection.execute(
                    text(f'''INSERT INTO public.COUNTRIES(COUNTRY) VALUES (NULL)'''))

                connection.execute(text(f'''INSERT INTO public.BOOLEAN_VALUES(REFUND) VALUES ('No')'''))
                connection.execute(text(f'''INSERT INTO public.BOOLEAN_VALUES(REFUND) VALUES ('Yes')'''))
                connection.execute(text(f'''INSERT INTO public.BOOLEAN_VALUES(REFUND) VALUES (NULL)'''))

                db_df_map = {k: v for (k, v) in dict(zip(
                    [f"{i}" for i in open('log_columns', 'r').read().split(' ')],
                    ['report_id'] + data.columns.tolist())).items() if k != 'REPORT_ID'
                             }

                entity_map = {'App Name': pd.read_sql('SELECT * FROM public.APP_NAMES', engine),
                              'Subscription Name': pd.read_sql('SELECT * FROM public.SUBSCRIPTION_NAMES', engine),
                              'Subscription Duration': pd.read_sql('SELECT * FROM public.SUBSCRIPTION_DURATIONS',
                                                                   engine),
                              'Customer Currency': pd.read_sql('SELECT * FROM public.Currencies', engine),
                              'Proceeds Currency': pd.read_sql('SELECT * FROM public.Currencies', engine),
                              'Device': pd.read_sql('SELECT * FROM public.DEVICES', engine),
                              'Country': pd.read_sql('SELECT * FROM public.COUNTRIES', engine),
                              'Refund': pd.read_sql('SELECT * FROM public.BOOLEAN_VALUES', engine)}

                data[
                    ['Subscriber ID', 'App Apple ID', 'Subscription Apple ID', 'Subscription Group ID']
                ] = data[['Subscriber ID', 'App Apple ID',
                          'Subscription Apple ID', 'Subscription Group ID']].astype(str)

                for row in data.iterrows():
                    row = row[1]
                    insert_string = 'INSERT INTO apple_reports('
                    values_string = ' VALUES ('
                    for (db_key, df_key) in db_df_map.items():
                        if df_key in entity_map.keys():
                            entity = entity_map[df_key]
                            if 'currency' in db_key.lower():
                                index = 'currency_id'
                                value = 'currency'
                                entity_index = entity.loc[entity[value] == row[df_key],
                                                          index]
                            else:
                                entity_index = entity.loc[(entity['_'.join(df_key.split(' ')).lower()] == row[df_key]) |
                                                          (entity['_'.join(df_key.split(' ')).lower()].isna()),
                                                          db_key.lower()]
                            if len(entity_index.values) == 0:
                                row[df_key] = np.nan
                            else:
                                row[df_key] = entity_index.values[0]

                        insert_string += f'{db_key},'
                        if row[df_key] == ' ':
                            row[df_key] = np.nan

                        if type(row[df_key]) == str:
                            values_string += f"'{row[df_key]}',"
                        else:
                            values_string += f"{row[df_key]},"
                    execute = insert_string[:-1] + ')' + values_string[:-1] + ')'
                    execute = execute.replace('nan', 'NULL')
                    print(execute)
                    connection.execute(text(execute))

    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)
