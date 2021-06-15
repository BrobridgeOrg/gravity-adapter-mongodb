# gravity-adapter-mysql

Gravity adapter for MySQL

---

### Requirement
* mongodb must be replica mode or shard mode

initial replica
```bash
	mongo
	rs.initiate()
```
### Run docker add host
```
docker run --add-host mongodb-0.mongodb.converg-ot.svc.cluster.local:192.168.1.97 -it brobridgehub/gravity-adapter-mongodb:v1
```
