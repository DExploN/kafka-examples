FROM php:8.2-cli

# Аргументы для создания пользователя
ARG PHP_USER=phpuser
ARG PHP_UID=1000
ARG PHP_GID=1000

# Установка зависимостей
RUN apt-get update && apt-get install -y \
    git \
    unzip \
    librdkafka-dev \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Установка расширения rdkafka
RUN pecl install rdkafka && \
    docker-php-ext-enable rdkafka

# Установка Composer
COPY --from=composer:latest /usr/bin/composer /usr/bin/composer

# Создание пользователя и группы с указанными UID/GID
RUN groupadd -g ${PHP_GID} ${PHP_USER} && \
    useradd -u ${PHP_UID} -g ${PHP_GID} -m -s /bin/bash ${PHP_USER} && \
    echo "${PHP_USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Создание директории для проекта и назначение прав
RUN mkdir -p /var/www/html && \
    chown -R ${PHP_USER}:${PHP_USER} /var/www/html

WORKDIR /var/www/html

# Переключение на созданного пользователя
USER ${PHP_USER}

# Использование tail -f /dev/null для поддержания контейнера в рабочем состоянии
CMD ["tail", "-f", "/dev/null"]
