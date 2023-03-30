# Описание задачи 

Ваш агрегатор для доставки еды набирает популярность и вводит новую опцию — подписку. Она открывает для пользователей ряд возможностей, одна из них которых — добавлять рестораны в избранное. Только тогда пользователю будут поступать уведомления о специальных акциях с ограниченным сроком действия. Систему, которая поможет реализовать это обновление, вам и нужно будет создать.
Благодаря обновлению рестораны смогут привлечь новую аудиторию, получить фидбэк на новые блюда и акции, продать профицит товара и увеличить продажи в непиковые часы. Акции длятся недолго, всего несколько часов, и часто бывают ситуативными, а значит, важно быстро доставить их кругу пользователей, у которых ресторан добавлен в избранное.  
Система работает так:

    Ресторан отправляет через мобильное приложение акцию с ограниченным предложением. Например, такое: «Вот и новое блюдо — его нет в обычном меню. Дарим на него скидку 70% до 14:00! Нам важен каждый комментарий о новинке».
    Сервис проверяет, у кого из пользователей ресторан находится в избранном списке.
    Сервис формирует заготовки для push-уведомлений этим пользователям о временных акциях. Уведомления будут отправляться только пока действует акция.

# Описание решения

В соответствии с заданием был разработан скрипт на Python для получения, обработки и вывода данных входящего стримминга в различные источники - таблицу Postgres и топик Kafka. 
## Входные параметры скрипта: 
    TOPIC_NAME_IN - топик, из которого читаем сообщения, по умолчанию student.topic.cohort5.GRR.in
    TOPIC_NAME_OUT - топик, в который отправляем сообщения, по умолчанию student.topic.cohort5.GRR.out
## Использованные библиотеки
    datetime, pyspark, sys

## Описание работы скрипта    
* Обработка входящих параметров
* Запуск Spark
* Чтение потоковых данных из входящего топика Kafka
    - чтение сырых данных
    - перевод бинарных данных в текстовую форму
    - десериализация json
    - фильтр данных в соответствии с заданием: остаются только акции, актуальные в данные момент
    - дедупликация данных
* Чтение статических данных из таблицы Postgres subscribers_restaurants
    * дедупликация данных     
* Объединение (join) данных из Postgres с данными из Kafka
* Одновременная отправка полученного датафрейма при помощи foreachBatch 
    * пересылка в выходной топик Kafka
    * добавление пустой колонки feedback и загрузка в таблицу subscribers_feedback в Postgres

## Тестирование
Все тестовые скрипты и файлы находятся в папке src/scripts/testing. 
Для генерации тестовых сообщений Kafka был написан скрипт create_test_data.py, на базе открытого API uuidgenerator.net. Входные параметры скрипта: количество тестовых сообщений и папка для хранения файлов. Скрипт создает файлы: 
   * SQL файл для создания и заполнения таблицы subscribers_restaurants (restaurants.sql)
   * SQL файл для создания таблицы subscribers_feedback (feedback_DDL.sql)
   * тестовые сообщения для Kafka (kafka_text.txt)
В файле bash_scripts.sh перечислены все использованные команды bash. В файле Kafka.config содержатся параметры конфигурации Kafka. Файлы, созданные скриптом, находятся в папке src/scripts/testing/test_data.