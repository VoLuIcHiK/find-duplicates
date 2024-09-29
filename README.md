# <p align="center"> ЦИФРОВОЙ ПРОРЫВ: СЕЗОН ИИ </p>
# <p align="center"> ПОИСК ДУБЛИКАТОВ ВИДЕО </p>
<p align="center">
<img width="800" height="600" alt="Титу ридми" src="https://github.com/user-attachments/assets/13479f13-9ca8-4c04-aad5-c278a8b73081">
</p>


*Состав команды "нейрON"*   
*Чиженко Леон (https://github.com/Leon200211) - Fullstack-разработчик*    
*Сергей Куликов (https://github.com/MrMarvel) - Backend-разработчик*  
*Карпов Даниил (https://github.com/Free4ky) - ML-engineer/MLOps*  
*Валуева Анастасия (https://github.com/VoLuIcHiK) - Team Lead/Designer/Project manager*   
*Козлов Михаил (https://github.com/Borntowarn) - ML-engineer/MLOps*  

## Оглавление
1. [Задание](#1)
2. [Решение](#2)
3. [Результат разработки](#3)
4. [Уникальность нашего решения](#5)
5. [Стек](#6)
6. [Инструкция по запуску кода](#7)
7. [Ссылки](#9)

## <a name="1"> Задание </a>
На основе последовательности публикуемых видео, с применением технологий искусственного интеллекта, создать MVP в виде сервиса по определению дубликатов видео. Необходимо складывать информацию об обработанных видео в БД, при публикации нового видео надо ответить является ли оно дублем или нет.

## <a name="2">Решение </a>

Нам удалось разработать сервис, который позволяет находить дубликаты видео, основываясь не только на изображении, но и на аудио. 
Мы попробовали много подходов и моделей и решили остановиться вот на следующем пайплайне:
1. Разбиваем видео на кадры.
2. Ищем похожие кадры по предыдущим видео.
3. Если находим совпадение - сравниваем аудио (данный пункт нужен для того, чтоб обнаруживать сложные случаи, например, мужчина-астролог почти в одних и тех же костюмах на одном и том же фоне записывает ролики о разных знаках зодиака). Если видео похожи, а аудио нет - то это не дубликат, а сложный случай.
4. Если у нас разные видео, но похожие аудио - это сложный случай, но не дубликат (например, разные девушки танцуют под один и тот же трек)

Использование как самих кадров, так и аудио позволило значительно повысить точность работы системы.  
Сама система выглядит следующим образом:
<p align="center">
<img width="422" alt="Снимок экрана 2024-09-29 в 01 22 55" src="https://github.com/user-attachments/assets/69cf5907-aa19-4a9f-9b5c-a677511eabb7">
</p>

## <a name="3">Результат разработки </a>

В ходе решения поставленной задачи нам удалось реализовать *рабочий* прототип со следующими компонентами:
1. Сконвертированная в TensorRT модель 3D-CSL;
2. Отдельный алгоритм для анализа аудио
3. Очереди RabbitMQ для асинхронной обработки;
4. Triton Server, на котором развернута модель;
5. Наша модель отлично справляется с более чем 14 видами дубликатов;
6. Обработчики - связующее звено между моделью и очередями.


## <a name="5">Уникальность нашего решения </a>
- Реализация очередей с помощью контейнера RabbitMQ;
- Развертыввание модели на Triton Server;
- Отдельный алгоритм для анализа спектрограмм;
- Ускорение работы за счет использования TensorRT;
- Высокая скорость работы модели - менее 5 секунд на обработку почти минутного видео;
- F1-score более 0.9 на тестовом датасете;
- Отличное соотношение время/качество работы;
- Готовый для масштабирования и интеграции прототип.

## <a name="6">Стек </a>
<div>
  <img src="https://github.com/devicons/devicon/blob/master/icons/python/python-original-wordmark.svg" title="Python" alt="Puthon" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/css3/css3-plain-wordmark.svg" title="css" alt="css" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/javascript/javascript-original.svg" title="js" alt="js" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/html5/html5-original-wordmark.svg" title="html" alt="html" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/php/php-original.svg" title="php" alt="php" width="40" height="40"/>&nbsp;
  <img src="https://github.com/devicons/devicon/blob/master/icons/docker/docker-original-wordmark.svg" title="docker" alt="docker" width="40" height="40"/>&nbsp;
  <img src="https://github.com/leungwensen/svg-icon/blob/master/dist/svg/logos/rabbitmq.svg" title="RabbitMQ" alt="RabbitMQ" width="40" height="40"/>&nbsp;
  <img src="https://github.com/vinceliuice/Tela-icon-theme/blob/master/src/scalable/apps/nvidia.svg" title="Triton" alt="Triton" width="40" height="40"/>&nbsp;

## <a name="7"> Инструкция по запуску кода </a>
### 1. Обращение по API
- Откройте Swagger страницу в браузере по адресу: http://87.236.30.202:8054/docs
- Веб интерфейс доступен в браузере по ссылке: http://87.236.30.202:8080
  
### 2. Локальный запуск
#### [⚠️] Нужно иметь установленный Docker и Docker Compose.
1. Сконвертируте модель в формат transformers с помощью notebooks/convert_model_to_transformers.ipynb
2. Сконвертируйте полученную модель в onnx с помощью notebooks/convert_onnx_transformers.ipynb
3. Сконвертиуруйте модель в формат tensorrt plan и поместите в model_repository/timesformer_dynamic_128_fp16_tensorrt/1

#### Далее выполните команду:
> docker-compose up -d  

#### Далее открыть Swagger страницу в браузере по адресу: http://localhost:8054/docs. 
#### Созданный эндпоинт совпадает с заданием, можно протестировать его работу.

### Тестирование модуля fastapi
#### Для тестирования модуля fastapi нужно выполнить команду:
> docker-compose -f docker/fastapi-selfloop.compose.yaml up -d

Далее открыть Swagger страницу в браузере по адресу: http://localhost:8054/docs и протестировать работу эндпоинта.  
Будут возвращаться случайные значения.

Версия с корректрными путями к volume находится в ветке free4ky

## <a name="9">Ссылки</a>
- [Гугл диск с материалами](https://drive.google.com/drive/folders/1eZOUoq9VwWq9YPDzA4q11AcCsS-dLl73?usp=sharing)
