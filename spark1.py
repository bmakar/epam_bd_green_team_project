# coding: utf-8
from pyspark import SparkContext as sc
from pyspark.sql.functions import concat_ws
import pyspark
import sys
# sys.setdefaultencoding() does not exist, here!
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')
sqlContext = pyspark.SQLContext(pyspark.SparkContext())
df = sqlContext.read.json('file:///root/rezerv/output.json')
df.select('geometry.location.lat', 'geometry.location.lng', 'name', concat_ws(',', 'types'), 'rating', 'vicinity').write.csv('/datastorage/poidata.csv')
