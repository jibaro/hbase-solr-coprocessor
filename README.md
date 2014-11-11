hbase-solr-coprocessor
======================

测试代码，目的是借助solr实现hbase二级索引，以使hbase支持高效的多条件查询。主要通过hbase的coprocessor的Observer实现，通过coprocessor在记录插入hbase时向solr中创建索引。

项目核心为SolrIndexCoprocessorObserver，该类继承BaseRegionObserver，并实现postPut和postDelete方法，以实现hbase数据同步到solr。考虑到solr插入效率和频繁写入的问题，这里实现了一个简单的缓冲池，当达到最大提交时间或池满的情况下才向solr中写。