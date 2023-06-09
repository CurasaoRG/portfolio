## Описание задачи

Необходимо добавить в Data Lake хранилище новые данные и построить на их основе аналитическую витрину. 

Коллеги из другого проекта по просьбе вашей команды начали вычислять координаты событий (сообщений, подписок, реакций, регистраций), которые совершили пользователи соцсети. Значения координат будут появляться в таблице событий. Пока определяется геопозиция только исходящих сообщений, но уже сейчас можно начать разрабатывать новый функционал. 
В продукт планируют внедрить систему рекомендации друзей. Приложение будет предлагать пользователю написать человеку, если пользователь и адресат:

- состоят в одном канале,
- раньше никогда не переписывались,
- находятся не дальше 1 км друг от друга.

При этом команда хочет лучше изучить аудиторию соцсети, чтобы в будущем запустить монетизацию. Для этого было решено провести геоаналитику:

- Выяснить, где находится большинство пользователей по количеству сообщений, лайков и подписок из одной точки.
- Посмотреть, в какой точке Австралии регистрируется больше всего новых пользователей.
- Определить, как часто пользователи путешествуют и какие города выбирают.

Благодаря такой аналитике в соцсеть можно будет вставить рекламу: приложение сможет учитывать местонахождение пользователя и предлагать тому подходящие услуги компаний-партнёров. 

### Структура хранилища
* /user/master/data/geo/events - все события с геолокацией, загрузка инкрементальная, ежедневно.
* /user/crashmosco/test - папка для тестирования
* /user/crashmosco/data/events/ - данные загружены по дате, партицированы по типу события event_type
* /user/crashmosco/analytics - папка для аналитики
* /user/crashmosco/data/tmp/geo.csv - CSV файл с координатами городов
* /user/crashmosco/data/geoinfo - Parquet файл, собранный из CSV файла с координатами городов