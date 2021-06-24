#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# pylint: disable=missing-function-docstring
"""
### Custom Xcom back-end to support Pandas using Redis

This is a simple example to illustrate an option to extend Xcoms for use
with Pandas dataframes by storing Pandas on Redis and
passing them around between tasks.
This is intended as an example to use Redis as a storage mechanism for 
passing data around between tasks running on a distributed environment.
"""
from typing import Any
import uuid
import pandas as pd
from airflow.models.xcom import BaseXCom
import redis

PREFIX = 'XCOM_'

class CustomXcomRedisForPandas(BaseXCom):
    """Example Custom Xcom persistence class - extends base to support Pandas Dataframes."""

    @staticmethod
    def write_and_upload_value(value):
        key_str = PREFIX + str(uuid.uuid4())
        value = value.to_json(orient='split')
        r = redis.Redis()
        print('Uploading')
        r.mset({key_str:value})
        print("Xcom with key=",key_str, " Uploaded to Redis")
        return key_str

    @staticmethod
    def read_value(filename):
        # Here we download the file
        r = redis.Redis()
        data = r.mget(filename)
        value = pd.read_json(data, orient='split')
        print('Success in reading dataframe')
        return value

    @staticmethod
    def serialize_value(value: Any):
        if isinstance(value, pd.DataFrame):
            value = CustomXcomRedisForPandas.write_and_upload_value(value)
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)
        # Check if result is string and have XCOM as prefix
        if isinstance(result, str) and result.startswith(PREFIX):
            return CustomXcomRedisForPandas.read_value(result)
        return result
    
    def orm_deserialize_value(self):
        if self.key.startswith('df_'):
            return 'XCOM uploaded to Redis'
        return BaseXCom.deserialize_value(self)
