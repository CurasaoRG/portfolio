## Описание задачи. 
Необходимо спроектировать хранилище для сбора и обработки данных из разных источников:  система обработки заказов (Postgre SQL), система оплаты баллами (MongoDB), система доставки заказов (web API). 
На данных из хранилища нужно построить витрины для аналитиков:
- dm_settlement_report, содержит информацию о суммах выплат ресторанам-партнёрам,
- dm_courier_ledger, содержит информацию о расчетах с курьерами. 
Подробное содержание витрин в файле [reports.md](/de_project_5/reports.md). 
Заполнение витрин и хранилища должно быть автоматизировано силами оркестратора Airflow. 

## Описание решения

В хранилище представлены три слоя: STG, DDS, CDM. 
В STG данные изо всех источников загружаются "as is", в DDS применяется модель данных "снежинка", CDM - слой витрин. 
Данные в слои STG и DDS загружаются инкрементально, информация об инкрементах содержится в технических таблицах srv_wf_settings.
В слое CDM были спроектированы витрины:
- dm_settlement_report, содержащая информацию о суммах выплат ресторанам-партнёрам.
- dm_courier_ledger, содержащая информацию о расчетах с курьерами. 

### Более подробное описание DWH процесса загрузки из API.
Процесс состоит из следующих этапов: 
* Загрузка сырых данных из API в STG схему. Данных загружались as is, DDL stg-таблиц в файле [src/SQL/STG_tables_DDL.sql](/de_project_5/src/SQL/STG_tables_DDL.sql). Из-за технических ограничений API и для снижения нагрузки на него, загрузка велась инкрементами, для отслеживания статуса использовалась таблица stg.srv_wf_settings.
* Обработка и  загрузка данных в таблицы измерений и фактов в схеме DDS. В качестве модели данных была выбрана модель snowflake ("Снежинка"). Были созданы таблицы измерений: dds.dm_api_couriers, dds.dm_api_restaurants, dds.dm_api_address, dds.dm_api_orders, dds.dm_delivery_details и таблица фактов dds.fct_api_sales. DDL скрипты для создания таблиц находятся в файле [SQL/DDS_tables_DDL.sql](/de_project_5/src/SQL/DDS_tables_DDL.sql). Схема модели данных в формате vuerd в файле [SQL/DDS.vuerd.json](/de_project_5/src/SQL/DDS.vuerd.json). Загрузка данных в таблицы измерений и фактов велась инкрементами, для отслеживания статуса использовалась таблица dds.srv_wf_settings.
* Загрузка агрегированных данных в витрину dm_courier_ledger в CDM. Было создано вспомогательное представление cdm.courier_agg_with_rates и таблица витрины cdm.dm_courier_ledger. 

Для автоматизации использовался оркестратор Airflow. Задачи написаны на языке Python, исходный код DAG в файле Sprint5_project_dag.py. Использовались следующие библиотеки Python: datetime, airflow, psycopg2, pendulum, requests, json.