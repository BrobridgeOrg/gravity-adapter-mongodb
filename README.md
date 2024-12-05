# gravity-adapter-mongodb

Gravity adapter for MongoDB
## Requirment
- MongoDB 3.6 and higher.
- MongoDB must be replica set or shard mode

## config.toml 說明
##### configs/config.toml example
```
[gravity]
domain = "default"
host = "localhost"
port = 4222
pingInterval = 10
maxPingsOutstanding = 3
maxReconnects = -1
accessToken = ""
publishBatchSize = 1000
rateLimit=0

[adapter]
adapterID = "mongodb_adapter"
adapterName = "Mongodb Adapter"

[source]
config = "./settings/sources.json"

[store]
enabled = true
path = "./statestore"
```

|參數|說明|
|---|---|
|gravity.domain| 設定gravity domain |
|gravity.host | 設定 gravity 的 nats ip |
|gravity.port | 設定 gravity 的 nats port |
|gravity.pingInterval | 設定 gravity 的 pingInterval |
|gravity.maxPingsOutstanding | 設定 gravity 的 maxPingOutstanding |
|gravity.maxReconnects | 設定 gravity 的 maxReconnects |
|gravity.accessToken | 設定 gravity 的 accessToken (for auth) |
|gravity.publishBatchSize | 設定 adapter 發送 Event 至 nats 時 累積多少筆資料進行發送狀態檢查 |
|gravity.rateLimit | 設定 adapter 發送 Event 至 nats 時 每秒速率上限 預設為 0 表示不限制 |
|source.config |設定 Adapter 的 來源設定檔位置 |
|store.enabled |是否掛載 presistent volume (記錄狀態) |
|store.path | 設定 presistent volume 掛載點 (記錄狀態) |


> **INFO**
>
 config.toml 設定可由環境變數帶入，其環境變數命名如下：
 **GRAVITY\_ADAPTER\_MONGODB** + \_ + **[section]** + \_ + **key**
 其中所有英文字母皆為大寫並使用_做連接。
>
 YAML 由環境變數帶入 gravity.host 範例:
>
```
env:
- name: GRAVITY_ADAPTER_MONGODB_GRAVITY_HOST
  value: 192.168.0.1
```

## settings.json 說明
##### settings/sources.json example
```
{
  "sources": {
    "my_mongo": {
      "disabled": false,
      "uri": "mongodb://mongodb-0:27017,mongodb-1:27017,mongodb-2:27017/?replicaSet=mongodb&tls=true",
      "ca_file": "rootCA.pem",
      "username": "admin",
      "password": "1qaz@WSX",
      "authSource": "admin",
      "dbname": "admin",
      "initialLoad": false,
      "tables": {
        "account": {
          "events": {
            "//comment": "no support snapshot event, initialLoad events are in create / update /delete",
            "//snapshot": "accountInitialized",
            "create": "accountCreated",
            "update": "accountUpdated",
            "delete": "accountDeleted"
          }
        }
      }
    }
  }
}
```

|參數|說明 |
|---|---|
| sources.SOURCE_NAME.disabled | 是否停用這個source |
| sources.SOURCE_NAME.url |設定mongodb connection url，可參考 [Connection URI](https://www.mongodb.com/docs/drivers/go/current/fundamentals/connections/connection-guide/#connection-uri) |
| sources.SOURCE_NAME.ca_file |啟動tls=true時，設定ca憑證路徑 |
| sources.SOURCE_NAME.username |設定 mongodb 登入帳號 |
| sources.SOURCE_NAME.password |設定 mongodb 登入密碼 |
| sources.SOURCE_NAME.authSource |設定使用者認證的資料庫名稱 |
| sources.SOURCE_NAME.dbname | 設定 mongodb database name |
| sources.SOURCE_NAME.initialLoad |  是否同步既有 record （在初始化同步時禁止對該資料表進行操作） |
| sources.SOURCE_NAME.tables.COLLECTION\_NAME | 設定要捕獲事件的 collection 名稱 |
| sources.SOURCE_NAME.tables.COLLECTION\_NAME.event.create | 設定 create event name |
| sources.SOURCE_NAME.tables.COLLECTION\_NAME.event.update | 設定 update event name |
| sources.SOURCE_NAME.tables.COLLECTION\_NAME.event.delete | 設定 delete event name |

> **INFO**
>
 資料庫的連線密碼可由環境變數帶入(需要使用工具做 AES 加密)，其環境變數如下：
  **[SOURCE_NAME] + \_ + PASSWORD**
>
Kubernetes 可以用 secret 帶入環境變數，範例如下:
```
...
- env:
  - name: MONGODB_SOURCE_PASSWORD
    valueFrom:
      secretKeyRef:
        name: mongodb-password
        key: db_source_mongodb_password
...
---
apiVersion: v1
kind: Secret
metadata:
  name: mongodb-password
type: Opaque
stringData:
  db_source_mongodb_password: 8e5ca8439ddebbac9bf2b83caf4bef7a

```
>
 settings.json 設定可由環境變數帶入，其環境變數如下：
 **GRAVITY\_ADAPTER\_MONGODB\_SOURCE\_SETTINGS**
>
 YAML 由環境變數帶入範例:
>
```
env:
- name: GRAVITY_ADAPTER_MONGODB_SOURCE_SETTINGS
  value: |
    {
      "sources": {
        "my_mongo": {
          "disabled": false,
          "uri": "mongodb://mongodb-0:27017,mongodb-1:27017,mongodb-2:27017/?   replicaSet=mongodb&tls=true",
          "ca_file": "rootCA.pem",
          "username": "admin",
          "password": "1qaz@WSX",
          "authSource": "admin",
          "dbname": "admin",
          "initialLoad": false,
          "tables": {
            "account": {
              "events": {
                "//comment": "no support snapshot event, initialLoad events are     in create / update /delete",
                "//snapshot": "accountInitialized",
                "create": "accountCreated",
                "update": "accountUpdated",
                "delete": "accountDeleted"
              }
            }
          }
        }
      }
    }
```

---

> **補充**
>
> 設定 Log 呈現的 Level 可由環境變數帶入:
其設定可使用 **debug**, **info**, **error**
>
```
env:
- name: GRAVITY_DEBUG
  value: debug
```

---
## Build
```
podman buildx build --platform linux/amd64 --build-arg="AES_KEY=**********" -t hb.k8sbridge.com/gravity/gravity-adapter-mongodb:v2.0.0 -f build/docker/Dockerfile .
```

---
## Deploy MongoDB Replica Set by Docker
Start MongoDB Instances
```
docker run -d --name mongo1 -p 27017:27017 mongo --replSet rs0 --bind_ip_all
```
Initiate the Replica Set
```
docker exec -it mongo1 mongosh --eval "rs.initiate({
 _id: \"rs0\",
 members: [
   {_id: 0, host: \"mongo1\"}
 ]
})"
```
Test and Verify the Replica Set
```
docker exec -it mongo1 mongosh --eval "rs.status()"
```

---
## Deploy gravity-adapter-mongodb on Kubernetes
### 設定 mongodb 連線資訊並部署
1. 在 [samples/gravity-adapter-mongodb.yaml](samples/gravity-adapter-mongodb.yaml) 修改 GRAVITY_ADAPTER_MONGODB_SOURCE_SETTINGS setting json 
2. kubectl apply 部署
   ```
   kubectl --namespace <my-namespace> apply -f samples/gravity-adapter-mongodb.yaml
   ``` 
3. 確認部署完畢
   ```
   kubectl --namespace <my-namespace> get pods
   ```

### 設定 mongodb tls 連線方式
1. 將 ca 憑證建立configmap
   ```
   kubectl --namespace <your-namespace> create configmap ca-config-map --from-file=ca.crt=<path-to-ca.crt>
   ```
2. 在 [samples/gravity-adapter-mongodb_tls.yaml](samples/gravity-adapter-mongodb_tls.yaml) 修改 GRAVITY_ADAPTER_MONGODB_SOURCE_SETTINGS setting json
   - 在 url 加入 tls=true 參數
   - 在 ca_file 指定憑證路徑
3. kubectl apply 部署
   ```
   kubectl --namespace <my-namespace> apply -f samples/gravity-adapter-mongodb_tls.yaml
   ```
4. 確認log成功連線
   ```
   kubectl --namespace <my-namespace> logs gravity-adapter-example-0
   ```

---
## License
Licensed under the MIT License

## Authors
Copyright(c) 2024 Fred Chien <<fred@brobridge.com>>  
Copyright(c) 2024 Jhe Sue <<jhe@brobridge.com>>  
Copyright(c) 2024 Jose Wang <<jose@brobridge.com>>
