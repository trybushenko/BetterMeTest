Даний проект є частиною тестового завдання в BetterMe.
У директорії reports знаходяться звіти про встановлення застосунку від App Store.
Ціль - створити логічну та фізичну схеми бази даних для цих файлів та зробити 
запити, щоб удостовіритися, що все працює коректно.

Є два варіанти конекту з базою:
1) За допомогою докеру(варіант, за допомогою якого це завдання було виконане):
    1. Запускаєте команду у терміналі ```docker run --name artem-pg-13.3 -p 5432:5432 -e POSTGRES_USER=habrpguser -e POSTGRES_PASSWORD=pgpwd -e POSTGRES_DB=habrdb -d postgres:13.3```
    2. Вказуєте credentials у коді;
    3. Створюєте базу даних "Create database BetterMeTest";
    4. Запускаєте код і отримуєте результат;
    5. Насолоджуєтеся );
2) За допомогою глобального PostgreSQL, встановивши його собі на комп'ютер.;
    1. Визначаєте в налаштуваннях бази юзера, пароль до нього, порт, хост.;
    2. Створюєте базу "Create database BetterMeTest";
    3. Указуєте свої креди на початку коду.;
    4. Запускаєте код і отримуєте результат;
    5. Насолоджуєтеся )
    
Як запустити Tablea Dashboard:

0) Скачуєте файли з розширенням *.twb та *.csv;
1) Відкриваєте файл з розширенням *.twb;
2) Заходите знизу у вкладку Data Source та вказуєте на нещодавно скачаний файл з розширенням *.csv;
3) Заходите знизу на вкладку Dashboard;
4) Переглядаєте;
5) Насолоджуєтеся )
