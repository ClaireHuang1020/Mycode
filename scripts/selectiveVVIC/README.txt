# 拉取商品详细数据
_DEBUG=1 _ZK_HOSTS=10.20.14.68 _APP_NAME=dna _APP_ENV=dev python3 task_detail.py

# 将vvic商品信息导入perfee数据库
执行前，修改immigrate.py中ids的IP地址为对应环境的ids访问地址
_DEBUG=1 _ZK_HOSTS=10.20.14.68 _APP_NAME=dna _APP_ENV=dev python3 task_immigrate.py