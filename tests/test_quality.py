import contextlib
import importlib.util
import io
import tempfile
import unittest

@unittest.skipUnless(importlib.util.find_spec('pyspark'), 'requires the project Spark container')
class QualityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master('local[2]').appName('quality-regression')
                     .config('spark.sql.shuffle.partitions','2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    def frame(self, rows):
        return self.spark.createDataFrame(rows,
            'userId long, movieId long, rating double, timestamp long')
    def test_four_checks_boundaries_nan_and_conservation(self):
        from quality import check_ratings_spark
        df = self.frame([(1,1,0.5,100),(2,1,5.0,101),(3,1,None,102),
            (4,1,6.0,103),(-1,1,3.0,104),(5,1,float('nan'),105),
            (6,1,float('inf'),106),(7,1,float('-inf'),107),
            (8,1,2.5,108),(8,1,2.5,108)])
        good,bad,fails = check_ratings_spark(df)
        self.assertEqual(fails,dict(missing=1,range=4,ids=1,duplicates=1))
        self.assertEqual(good.count(),3)
        self.assertEqual(bad.count(),7)
        self.assertEqual(good.count()+bad.count(),df.count())
        with tempfile.TemporaryDirectory() as directory:
            bad.write.parquet(directory + '/quarantine')
            self.assertEqual(self.spark.read.parquet(directory + '/quarantine').count(),7)
    def test_invalid_duplicate_cannot_displace_valid_row(self):
        from quality import check_ratings_spark
        good,bad,fails = check_ratings_spark(self.frame([(1,1,9.0,100),(1,1,4.0,100)]))
        self.assertEqual([r.rating for r in good.collect()],[4.0])
        self.assertEqual(bad.count(),1)
        self.assertEqual(fails['duplicates'],1)
    def test_empty_batch(self):
        from quality import check_ratings_spark
        good,bad,fails=check_ratings_spark(self.frame([]))
        self.assertEqual(sum(fails.values()),0)
        self.assertEqual(good.count()+bad.count(),0)
    def test_malformed_csv_numbers_are_quarantined(self):
        from quality import check_ratings_spark
        schema = 'userId long, movieId long, rating double, timestamp long'
        with tempfile.TemporaryDirectory() as directory:
            path=directory + '/ratings.csv'
            with open(path,'w') as f:
                f.write('userId,movieId,rating,timestamp\n1,1,4.0,100\n2,1,oops,101\n')
            frame=self.spark.read.schema(schema).csv(path,header=True)
            good,bad,fails=check_ratings_spark(frame)
            self.assertEqual(fails['missing'],1)
            self.assertEqual((good.count(),bad.count()),(1,1))

@unittest.skipUnless(importlib.util.find_spec('pyspark'), 'requires project dependencies')
class ProducerValidationTest(unittest.TestCase):
    def test_malformed_values_and_duplicate_validation(self):
        from quality import row_failures
        valid=dict(userId=1,movieId=2,rating=4.0,timestamp=100)
        for field,value,label in [('rating','oops','range'),('rating',float('nan'),'range'),
            ('rating',float('inf'),'range'),('userId',0,'ids'),('timestamp','oops','ids'),
            ('movieId',True,'ids'),('rating',None,'missing')]:
            self.assertIn(label,row_failures({**valid,field:value}))
        seen=set()
        self.assertEqual(row_failures({**valid,'rating':9.0},seen),['range'])
        self.assertEqual(row_failures(valid,seen),[])
        self.assertEqual(row_failures(valid,seen),['duplicates'])
    def test_alert_boundary(self):
        from quality import alert_if_needed
        text=io.StringIO()
        with contextlib.redirect_stdout(text):
            alert_if_needed(1,100,'at-limit')
            alert_if_needed(0,0,'empty')
        self.assertEqual(text.getvalue(),'')
        with contextlib.redirect_stdout(text):
            alert_if_needed(2,100,'over-limit')
        self.assertIn('[ALERT]',text.getvalue())
