from django.conf.urls import url
from manager import views

urlpatterns = [
    url(r'cpu-check/$', views.CheckView.as_view(), name='cpu_check'),
    url(r'account/balance/$', views.AccountBalanceView.as_view(), name='account_balance'),
    url(r'para-user/list/$', views.UserListView.as_view(), name='para-user-list')
]
