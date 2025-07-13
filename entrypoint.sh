#!/bin/bash

# Ожидаем, пока база поднимется (если она появится в будущем)
# echo "Waiting for db..."
# sleep 5

# Применяем миграции
python manage.py migrate
# Запускаем сервер (заменяется в docker-compose через `command`, но на всякий случай)
exec "$@"
