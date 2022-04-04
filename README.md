# es7.x springboot使用案例

### es官方镜像下载地址
https://hub.docker.com/_/elasticsearch?tab=tags

### docker启动一个es7.17.2

docker run -d --name elasticsearch  -p 9200:9200 
-p 9300:9300 -e "discovery.type=single-node" 
elasticsearch:7.17.2

### 访问es
ip:9200


