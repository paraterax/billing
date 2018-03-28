from django.conf.urls import url
from manager import views

import collector.crontab

urlpatterns = [
    url(r'cpu-check/$', views.CheckView.as_view(), name='cpu_check')
]
