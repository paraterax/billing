"""
Django settings for billing_collect_new project.

Generated by 'django-admin startproject' using Django 1.11.11.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.11/ref/settings/
"""

import os

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.11/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'qavqnxtqy+cy(-)o%ulwbp_pel)=!x1eg-=$=q#ai0!yjr@#7u'

# SECURITY WARNING: don't run with debug turned on in production!
if os.environ.get('BILLING_COLLECT_DEBUG'):
    DEBUG = True
    ALLOWED_HOSTS = []
else:
    DEBUG = False
    ALLOWED_HOSTS = ['111.200.54.*']

# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_extensions',
    'django_celery_beat',
    'collector',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'billing_collect_new.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates')]
        ,
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'billing_collect_new.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases

if DEBUG:
    db_default_file = os.path.join(BASE_DIR, 'billing_collect_new/default_db_dev.cnf')
    job_mapping_file = os.path.join(BASE_DIR, 'billing_collect_new/job_mapping_dev.cnf')
else:
    db_default_file = os.path.join(BASE_DIR, 'billing_collect_new/default_db_prod.cnf')
    job_mapping_file = os.path.join(BASE_DIR, 'billing_collect_new/job_mapping_prod.cnf')

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'OPTIONS': {
            'read_default_file': db_default_file
        }
    },
    'job-mapping': {
        'ENGINE': 'django.db.backends.mysql',
        'OPTIONS': {
            'read_default_file': job_mapping_file
        }
    }
}


# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'Asia/Shanghai'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.11/howto/static-files/

STATIC_URL = '/static/'

COLLECTOR_CONFIG = {
    # 在找不到对应队列的单价时，是否继续扣费
    'IGNORE_MISSING_QUEUE': False,

    # 再找不到该用户的队列单价时，是否参照标准队列单价
    'STANDARD_PRICE': True
}

# Celery 在此配置
CELERY_BROKER_URL = "redis://"
CELERY_RESULT_BACKEND = "redis://"
# CELERY_TIMEZONE = TIME_ZONE
CELERY_ENABLE_UTC = False
CELERY_TASK_ROUTES = {
    'collector.tasks.*': {'queue': 'collect'},
    'collector.result.*': {'queue': 'result'}
}

CLUSTER_JOB_KEY = {
    "GUANGZHOU": "A",
    "ParaGrid1": "Grid",
    "ERA": "ERA",
    "PART1": "A",
    "LVLIANG": "A"
}

try:
    from billing_collect_new.crontab_config import *
except ImportError:
    print("Crontab config not found.")

try:
    from billing_collect_new.collect_config import *
except ImportError:
    print("Crontab config not found.")
