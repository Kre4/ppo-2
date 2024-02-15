# Порядок запуска
Бомбардир
* `docker-compose -f .\bombardier-main\docker-compose.yml up -d`
* Запустить `DemoServiceApplication.kt`

OnlineShopApp
* `docker-compose -f .\high-load-course-main\docker-compose.yml up -d`
* Запустить OnlineShopApplication.kt

Проверка работоспособности
* Выполнить запрос bombardier-main/run_tests.http
* Открыть http://localhost:3000/d/KVr-Vmpnz/services-statistic?orgId=1  (логин, пароль: `admin`, `quipy`)
* Увидеть что-то такое
![image](https://github.com/Kre4/ppo-2/assets/37481171/51194941-4255-4735-8f77-6271f637e16c)
