-- создаем пользователей для сервиса инвалидации
CREATE USER demo_replication_user WITH LOGIN PASSWORD 'demo_replication_pass' REPLICATION;
GRANT CONNECT ON DATABASE demo TO demo_replication_user;

-- создание пользователя для чтения LSN и проверки слота репликации
CREATE USER demo_user WITH LOGIN PASSWORD 'demo_pass' REPLICATION;
GRANT CONNECT ON DATABASE demo TO demo_user;
GRANT SELECT ON pg_replication_slots TO demo_user;

CREATE PUBLICATION demo_pub FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('tkcdc_slot', 'pgoutput');

-- инициализация демонстрационной таблицы
CREATE TABLE IF NOT EXISTS users (
	id SERIAL PRIMARY KEY,
	name TEXT NOT NULL,
	email TEXT NOT NULL
);

INSERT INTO users (name, email) VALUES
('Alice', 'alice@test.com'),
('Joe', 'cooljoe@test.com'),
('Bob', 'marley@test.com'),
('Skinny Pete', 'skinnypete@test.com'),
('Badger', 'badger@test.com'),
('Hank Schrader', 'schrader@test.com')
