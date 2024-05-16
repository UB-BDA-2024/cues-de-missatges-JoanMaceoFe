from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from shared.mongodb_client import MongoDBClient
from shared.elasticsearch_client import ElasticsearchClient 
from shared.timescale import Timescale
from shared.cassandra_client import CassandraClient


from . import models, schemas
import json
import time
from datetime import datetime

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def get_temperature_values(db: Session, cassandra: CassandraClient, timescale: Timescale):
    json_result = {"sensors": []}

    # Creem el KeySpace per al sensors
    query = "CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    cassandra.execute(query)
    
    cassandra.get_session().set_keyspace('sensor')
    # Creem la taula on farem les operacions amb les temperatures dels sensors
    cassandra.execute("CREATE TABLE sensor_temperatures (id int, temperature float, PRIMARY KEY (id, temperature))")

    # Agafem les dades de temperatura dels sensors
    timescale.execute("SELECT id , temperature FROM sensor_data WHERE temperature IS NOT NULL")
    sensor_data_result = timescale.getCursor().fetchall() 

    for i in sensor_data_result: 
        cassandra.get_session().execute("INSERT INTO sensor_temperatures (id, temperature) VALUES (%s, %s)", (i[0], i[1]))

    # Agafem el maxi, minim, y average de les temperatures
    result = cassandra.execute("SELECT id, MAX(temperature) AS max_temperature, MIN(temperature) AS min_temperature, AVG(temperature) AS avg_temperature FROM sensor_temperatures GROUP BY id")

    # Creem el json per al resultat de sortida
    for sensor_results in result:
        sensor_data = get_sensor(db, sensor_results[0])
        json_sensor = {"id": sensor_data.id, "name": sensor_data.name, "latitude": sensor_data.latitude, "longitude": sensor_data.longitude, "type": sensor_data.type, "mac_address": sensor_data.mac_address, "manufacturer": sensor_data.manufacturer, "model": sensor_data.model, "serie_number": sensor_data.serie_number, "firmware_version": sensor_data.firmware_version, "description": sensor_data.description, "values": []}
        json_sensor["values"].append({"max_temperature": sensor_results[1], "min_temperature": sensor_results[2], "average_temperature": sensor_results[3]})
        json_result["sensors"].append(json_sensor)
    
    return json_result


def get_sensors_quantity(db: Session, cassandra: CassandraClient):
    json_result = {"sensors": []}

    # Creem el KeySpace per al sensors
    query = "CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    cassandra.execute(query)
    
    cassandra.get_session().set_keyspace('sensor')
    # Creem la taula on contarem el total de sensors de cada tipus
    cassandra.execute("CREATE TABLE sensor_type (id int, type text, PRIMARY KEY (type, id)) WITH CLUSTERING ORDER BY (id DESC)")

    # Agafem tots els sensors de la base de dades
    all_sensors = get_sensors(db)

    for i in all_sensors:
        cassandra.get_session().execute("INSERT INTO sensor_type (id, type) Values (%s, %s)", (i.id, i.type))
    
    # Fem un count agrupant els sensors pel seu type
    query ="SELECT type, COUNT(*) FROM sensor_type GROUP BY type;"
    result = cassandra.execute(query)

    # Preparem el json per al resultat de sortida
    for sensor_results in result:
        json_result["sensors"].append({"type": sensor_results[0], "quantity": sensor_results[1]})
    
    return json_result


# Funció auxiliar per a fer les consultes sobre el nivell de bateria
def calculate_level_range(battery_level):
    if battery_level < 0.2:
        return "low"
    else:
        return "high"

def get_low_battery_sensors(db: Session, cassandra: CassandraClient, timescale: Timescale):
    json_result = {"sensors": []}

    # Creem el KeySpace per al sensors
    query = "CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    cassandra.execute(query)
    
    cassandra.get_session().set_keyspace('sensor')
    # Creem la taula on mirarem quins sensors tenen un nivell de bateria baix
    cassandra.execute("CREATE TABLE sensor_low_battery (level_range text, id int, battery_level float, PRIMARY KEY (level_range, id)) WITH CLUSTERING ORDER BY (id DESC)")

    # Agafem les dades del nivell de bateria dels sensors
    timescale.execute("SELECT id , battery_level FROM sensor_data")
    sensor_data_result = timescale.getCursor().fetchall() 

    for i in sensor_data_result: 
        cassandra.get_session().execute("INSERT INTO sensor_low_battery (level_range, id, battery_level) VALUES (%s, %s, %s)", (calculate_level_range(i[1]), i[0], i[1]))

    # Seleccionem els sensors que tinguin un nivell de bateria baix, es a dir, menys del 20%
    result = cassandra.execute("SELECT id , battery_level FROM sensor_low_battery WHERE level_range = 'low'")

    # Preparem el json per al resultat de sortida
    for sensor_results in result:
        sensor_data = get_sensor(db, sensor_results[0])
        json_sensor = {"id": sensor_data.id, "name": sensor_data.name, "latitude": sensor_data.latitude, "longitude": sensor_data.longitude, "type": sensor_data.type, "mac_address": sensor_data.mac_address, "manufacturer": sensor_data.manufacturer, "model": sensor_data.model, "serie_number": sensor_data.serie_number, "firmware_version": sensor_data.firmware_version, "description": sensor_data.description, "battery_level": round(sensor_results[1], 5)}
        json_result["sensors"].insert(0, json_sensor)
    
    return json_result



def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client:MongoDBClient, es: ElasticsearchClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude, type=sensor.type,
    mac_address=sensor.mac_address, manufacturer=sensor.manufacturer, model=sensor.model, serie_number=sensor.serie_number,
    firmware_version=sensor.firmware_version, description = sensor.description)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    mongodb_document = {"id": db_sensor.id, "name": sensor.name, "latitude": sensor.latitude, "longitude": sensor.longitude,
    "type": sensor.type, "mac_address": sensor.mac_address, "manufacturer": sensor.manufacturer, "model": sensor.model,
    "serie_number": sensor.serie_number, "firmware_version": sensor.firmware_version, "description": sensor.description}
    mongodb_database = mongodb_client.getDatabase("sensors")

    mongodb_collection = None
    if sensor.type == "Temperatura":
        mongodb_collection = mongodb_client.getCollection("sensors temperatura")
    elif sensor.type == "Velocitat":
        mongodb_collection = mongodb_client.getCollection("sensors velocitat")
    mongodb_collection.insert_one(mongodb_document)

    # Creem l'index de sensors
    es_index_name='sensors'
    es.create_index(es_index_name)
    # Creem el mapping per a l'index
    mapping = {
        'properties': {
            'id': {'type': 'float'},
            'name': {'type': 'text'},
            'latitude': {'type': 'float'},
            'longitude': {'type': 'float'},
            'type': {'type': 'text'},
            'mac_address': {'type': 'text'},
            'manufacturer': {'type': 'text'},
            'model': {'type': 'text'},
            'serie_number': {'type': 'text'},
            'firmware_version': {'type': 'text'},
        }
    }
    es.create_mapping(es_index_name, mapping)
    # Creem un document amb l'informació del sensor
    es_doc = {
        'id': db_sensor.id,
        'name': sensor.name,
        'latitude': sensor.latitude,
        'longitude': sensor.longitude,
        'type': sensor.type,
        'mac_address': sensor.mac_address,
        'manufacturer': sensor.manufacturer,
        'model': sensor.model,
        'serie_number': sensor.serie_number,
        'firmware_version': sensor.firmware_version,
        'description': sensor.description
    }

    es.index_document(es_index_name, es_doc)
    es.close()

    return db_sensor


def record_data(redis: Session, sensor_id: int, data: schemas.SensorData, timescale: Timescale) -> schemas.Sensor:
    db_sensordata = data

    if db_sensordata.temperature is not None:
        redis.set(f'sensor {sensor_id} temperature', db_sensordata.temperature)
        redis.set(f'sensor {sensor_id} name', f'Sensor Temperatura {sensor_id}')
    if db_sensordata.velocity is not None:
        redis.set(f'sensor {sensor_id} velocity', db_sensordata.velocity)
        redis.set(f'sensor {sensor_id} name', f'Sensor Velocitat {sensor_id}')
    if db_sensordata.humidity is not None:
        redis.set(f'sensor {sensor_id} humidity', db_sensordata.humidity)


        
    # Fem una relacio clau-valor amb la clau sent "sensor sensor_id" i el nom de la propietat
    #redis.set(f'sensor {sensor_id} name', db_sensordata.name)
    redis.set(f'sensor {sensor_id} id', sensor_id)
    redis.set(f'sensor {sensor_id} battery_level', db_sensordata.battery_level)
    redis.set(f'sensor {sensor_id} last_seen', db_sensordata.last_seen)

    values = (sensor_id, db_sensordata.velocity, db_sensordata.temperature, db_sensordata.humidity, db_sensordata.battery_level, db_sensordata.last_seen)
    # Insertem els valors a la taula sensor_data
    timescale.getCursor().execute("INSERT INTO sensor_data (id, velocity, temperature, humidity, battery_level, last_seen) VALUES (%s, %s, %s, %s, %s, %s)", values)
    timescale.execute("commit")

    return db_sensordata

def get_data(redis: Session, sensor_id: int, _from: str, to: str, bucket: str, timescale: Timescale) -> schemas.Sensor:
    """# Primer mirem si existeix un sensor amb aquest id
    id_sensor = redis.get(f'sensor {sensor_id} id')
    # Si no existeix retornem None
    if id_sensor is None:
        return None

    # Finalmente convertim els valors obtinguts en el seu tipus corresponent
    id_sensor = int(id_sensor)
    name = redis.get(f'sensor {sensor_id} name')    
    battery_level = float(redis.get(f'sensor {sensor_id} battery_level'))
    last_seen = redis.get(f'sensor {sensor_id} last_seen')
    sensor_json = {"id": id_sensor, "name": name, "battery_level": battery_level, "last_seen": last_seen}

    humidity = redis.get(f'sensor {sensor_id} humidity')
    temperature = redis.get(f'sensor {sensor_id} temperature')
    velocity = redis.get(f'sensor {sensor_id} velocity')

    if humidity is not None:
        sensor_json["humidity"] = float(humidity)
    if temperature is not None:
        sensor_json["temperature"] = float(temperature)
    if velocity is not None:
        sensor_json["velocity"] = float(velocity)"""
    
    sensor_json = []
    
    if bucket == "hour":
        # Eliminem la vista si ja existeix
        timescale.execute("DROP MATERIALIZED VIEW if EXISTS continous_aggregate_hourly")
        timescale.execute("commit")
        # Query per a crear la vista materialitzada
        query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS continous_aggregate_hourly( velocity, temperature, humidity, battery_level, last_seen )
        WITH (timescaledb.continuous) AS
        SELECT avg(velocity), avg(temperature), avg(humidity), avg(battery_level), time_bucket('1h', last_seen, 'UTC') 
        FROM sensor_data
        WHERE id = {sensor_id}
        GROUP BY time_bucket('1h', last_seen, 'UTC') 
        WITH NO DATA;
        """
        timescale.execute(query)
        timescale.execute("commit")
        # Frem refresh per a que obtingui les dades de la view
        timescale.getCursor().execute("CALL refresh_continuous_aggregate('continous_aggregate_hourly', '2020-01-01T00:00:00.000Z', '2020-01-31T00:00:00.000Z');")
        timescale.execute("commit")


        timescale.getCursor().execute(f"SELECT * FROM continous_aggregate_hourly WHERE time_bucket('1h', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z')  >= %s AND time_bucket('1h', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z')  <= %s;", (_from, to))
        result = timescale.getCursor().fetchall()
        print(result)
        
        for row in result:
            sensor_row = {"velocity": row[0], "temperature": row[1], "humidity": row[2], "battery_level": row[3], "last_seen": row[4]}
            sensor_json.append(sensor_row)

    elif bucket == "day":
        timescale.execute("DROP MATERIALIZED VIEW if EXISTS continous_aggregate_daily")
        timescale.execute("commit")
        query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS continous_aggregate_daily( velocity, temperature, humidity, battery_level, last_seen )
        WITH (timescaledb.continuous) AS
        SELECT sum(velocity), sum(temperature), sum(humidity), sum(battery_level), time_bucket('1day', last_seen, 'UTC')
        FROM sensor_data
        WHERE id = {sensor_id}
        GROUP BY time_bucket('1day', last_seen, 'UTC')
        WITH NO DATA;
        """
        timescale.getCursor().execute(query)
        timescale.execute("commit")
        timescale.getCursor().execute("CALL refresh_continuous_aggregate('continous_aggregate_daily', '2020-01-01T00:00:00.000Z', '2020-01-31T00:00:00.000Z');")
        timescale.execute("commit")


        timescale.getCursor().execute(f"SELECT * FROM continous_aggregate_daily WHERE time_bucket('1day', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z') >= %s AND time_bucket('1day', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z') <= %s;", (_from, to))
        result = timescale.getCursor().fetchall()
        print(result)
        
        for row in result:
            sensor_row = {"velocity": row[0], "temperature": row[1], "humidity": row[2], "battery_level": row[3], "last_seen": row[4]}
            sensor_json.append(sensor_row)
    
    elif bucket == "week":
        timescale.execute("DROP MATERIALIZED VIEW if EXISTS continous_aggregate_weekly")
        timescale.execute("commit")
        query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS continous_aggregate_weekly( velocity, temperature, humidity, battery_level, last_seen )
        WITH (timescaledb.continuous) AS
        SELECT avg(velocity), avg(temperature), avg(humidity), avg(battery_level), time_bucket(INTERVAL '1 week', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z')
        FROM sensor_data
        WHERE id = {sensor_id}
        GROUP BY time_bucket(INTERVAL '1 week', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z')
        WITH NO DATA;
        """
        print(sensor_id)
        timescale.execute(query)
        timescale.execute("commit")
        timescale.getCursor().execute("CALL refresh_continuous_aggregate('continous_aggregate_weekly', '2020-01-01T00:00:00.000Z', '2020-01-31T00:00:00.000Z');")
        timescale.execute("commit")


        timescale.getCursor().execute("SELECT * FROM continous_aggregate_weekly WHERE time_bucket(INTERVAL '1 week', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z') >= %s AND time_bucket(INTERVAL '1 week', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z') <= %s;", (_from, to))
        result = timescale.getCursor().fetchall()
        print(result)
        
        for row in result:
            sensor_row = {"velocity": row[0], "temperature": row[1], "humidity": row[2], "battery_level": row[3], "last_seen": row[4]}
            sensor_json.append(sensor_row)

    elif bucket == "month":
        timescale.execute("DROP MATERIALIZED VIEW if EXISTS continous_aggregate_monthly")
        timescale.execute("commit")
        query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS continous_aggregate_monthly( velocity, temperature, humidity, battery_level, last_seen )
        WITH (timescaledb.continuous) AS
        SELECT avg(velocity), avg(temperature), avg(humidity), avg(battery_level), time_bucket('30day', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z')
        FROM sensor_data
        WHERE id = {sensor_id}
        GROUP BY time_bucket('30day', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z')
        WITH NO DATA;
        """
        timescale.execute(query)
        timescale.execute("commit")
        timescale.getCursor().execute("CALL refresh_continuous_aggregate('continous_aggregate_monthly', '2020-01-01T00:00:00.000Z', '2020-03-01T00:00:00.000Z');")
        timescale.execute("commit")


        timescale.getCursor().execute(f"SELECT * FROM continous_aggregate_monthly WHERE time_bucket('30day', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z') >= %s AND time_bucket('30day', last_seen, 'UTC', TIMESTAMPTZ '2020-01-01T00:00:00Z') <= %s;", (_from, to))
        result = timescale.getCursor().fetchall()
        print(result)
        
        for row in result:
            sensor_row = {"velocity": row[0], "temperature": row[1], "humidity": row[2], "battery_level": row[3], "last_seen": row[4]}
            sensor_json.append(sensor_row)

    return sensor_json

def search_sensors(db: Session, mongodb: MongoDBClient, query: str, size: int, search_type: str, es: ElasticsearchClient):
    results = None
    response = []
    query_es = {}
    # Agafem els valors de la query
    query_type = json.loads(query)
    key, value = next(iter(query_type.items()))
    # Mirem quin tipus de search es
    if search_type == "match":  
        query_es = {
            'query': {
                search_type: {
                    key: value
                }
            }, 
            'size': size
        }
        results = es.search("sensors", query_es)
        # Aquest if amb espera és perque de vegades no agafa bé la search a la primera, asi que esperem un segon i ho tornem a 
        # intentar una vegada més
        if results['hits']['hits'] == []:
            time.sleep(1)
            results = es.search("sensors", query_es)

    elif search_type == "similar":
        query_es= {
            'query': {
                'more_like_this': {
                    'fields': [key],
                    'like': value,
                    'min_term_freq': 1,
                    'min_doc_freq': 1,
                    'max_query_terms': 12
                }
            },
        }
        results = es.search("sensors", query_es)
        if results['hits']['hits'] == []:
            time.sleep(1)
            results = es.search("sensors", query_es)
        # Aquest if es per si hi ha més d'un document, aleshores fem un altre search i mirem que el score dels
        # documents no estigui més de 0,05 punts més lluny que el max_score   
        if len(results['hits']['hits']) > 1:
            query_es= {
                'query': {
                    'more_like_this': {
                        'fields': [key],
                        'like': value,
                        'min_term_freq': 1,
                        'min_doc_freq': 1,
                        'max_query_terms': 12
                    }
                },
                'min_score': results['hits']['max_score'] - 0.05
            }
            results = es.search("sensors", query_es)

    elif search_type == "prefix":
        query_es = {
            'query': {
                'match_phrase_prefix': {
                    key: {'query': value}
                }
            }
        }
        results = es.search("sensors", query_es)
        if results['hits']['hits'] == []:
            time.sleep(1)
            results = es.search("sensors", query_es)
    
    for hit in results['hits']['hits']:
        result = {"id": hit['_source']['id'], "name": hit['_source']['name'], "latitude": hit['_source']['latitude'], "longitude": hit['_source']['longitude'], "type": hit['_source']['type'], 
                  "mac_address": hit['_source']['mac_address'], "manufacturer": hit['_source']['manufacturer'], "model": hit['_source']['model'], "serie_number": hit['_source']['serie_number'], 
                  "firmware_version": hit['_source']['firmware_version'], "description": hit['_source']['description']}
        response.append(result)
    return response

def delete_sensor(sensor_id: int, db: Session, mongodb_client: MongoDBClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()

    mongodb_database = mongodb_client.getDatabase("sensors")
    mongodb_collection_velocitat = mongodb_client.getCollection("sensors velocitat")
    mongodb_collection_temperatura = mongodb_client.getCollection("sensors temperatura")
    mongodb_collection_velocitat.delete_one({"id": sensor_id})
    mongodb_collection_temperatura.delete_one({"id": sensor_id})
    return db_sensor

def get_sensors_near(mongodb: MongoDBClient, redis_client: Session, latitude: float, longitude: float):
    # criteri de cerca
    query = {"latitude": latitude, "longitude": longitude}
    # Busquem a les dues coleccions
    mongodb_database = mongodb.getDatabase("sensors")
    mongodb_collection_velocitat = mongodb.getCollection("sensors velocitat")
    sensors_velocitat = mongodb_collection_velocitat.find(query)
    mongodb_collection_temperatura = mongodb.getCollection("sensors temperatura")
    sensors_temperatura = mongodb_collection_temperatura.find(query)

   # Afegim cada cerca al resultat 
    sensors = []
    for sensor_temperatura in sensors_temperatura:
        sensors.append(get_data(redis_client, sensor_temperatura['id']))
    
    for sensor_velocitat in sensors_velocitat:
        sensors.append(get_data(redis_client, sensor_velocitat['id']))
    

    return sensors