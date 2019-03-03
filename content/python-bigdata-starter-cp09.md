---
title: "Python BigData Starter CP09"
cover: "1.jpg"
category: "python"
date: "2019-03-03"
slug: "python-bigdata-starter-cp09"
tags:
---​


​       当我们顺利从data中提炼了information以后，我们就要考虑如何将报表拷贝到数据库中，所以如何选择一款适合的高性能数据库，是本章节的重点。我们将选择postgres-SQL来配合python进行大数据协同开发。



# 9 基于postgres-SQL 的装载器

​         经过上一个章节的报表生成服务，我们基于pandas强大的数据加工功能获得了站点级别的报表和用户级别的报表。为了方便后续对数据进行综合应用，我们将考虑将将数装载到数据库当中。

​         在本case中，我们将使用postgres-sql数据库，这是一种非常强大的关系型数据库，同时对半结构化的Json文件也可以很好的支撑，所以在后续的大数据专案的应用中，我们将以postgres-sql 11版作为核心的数据库。



## 9.1   需求分析

### 9.1.1 用例回顾

 1，timer定期扫描结果集合文件。如果结果集文件存在，则准备进行数据入库操作。

2，timer 读取数据库连接配置数据，并启动数据库连接。

3，timer识别报表文件的文件名， 根据文件名映射出期望入库的表名，以及相关的表分区信息。

4，timer 将结果集文件传送到数据库当中，并确定输入的表名称和相关分片名称。

5，数据拷贝完毕以后，timer就释放数据连接。



### 9.1.2  数据库前置作业

​      为了实现数据的入库操作， 我们需要先创建数据库的表，期望创建的表的信息结构见下图：

1，站点级别的报表

![9-00 re1](/assets/python-bigdata-starter/cp09/9-00 re1.png)



   2、用户级别的报表

   ![9-01 re2](/assets/python-bigdata-starter/cp09/9-01 re2.png)



相关的文件名称分别是：

QLR201902071000-merged-re-site.csv

QLR201902071000-merged-re-customer.csv

对应的表名称分是

**re-site**   

**re-customer**

另外考虑到数据的规模可能很大，所以采用天粒度的分片策略。



### 9.1.3 输入（I）

   输入的数据为 [load directory, pg_conn_cfg.py]

   输入的数据包含两个元素：

1,  load directory： 装载数据的路径， 需要传入数据库的文件，全部放置在这个 load directory

2,  db_cfg 期望连接的数据库 信息。

相关的数据采用dict方式来表达。

pg_conn_dict = {

​     "host": "10.x.x.x",

​     "dbname": "bank",

​     "user":"postgres",

​     "password":"root"

}

  其中包含了用于连接数据库的配置信息



### 9.1.4  处理过程（P）

下面将相关的处理过程，简要说明一下：

1、          应用程度读取数据库的配置程序。

2、          根据配置信息，链接数据库。如果连接不成功，则反馈报错信息。

3、          应用程度扫描loader 路径。将待入库的文件名传入1个list。

4、          应用程序选择一个文件， 根据文件名，确定期待入库的表，和分片，将文件拷贝转载在对应的表中，并记载入库的记录数。

5、          应用程序将所有的文件装载完毕，生出相关的结果对象。

6、          应用程序释放数据库连接。



### 9.1.5   输出结果（O）

   输出的结果是[ DB_connect result, DBconnnect_log],

[[ filename1, result, load_log, No of record], [ filename2, result, load_log,No of record]]

**1,数据库连接结果**

DB_connect result： 0 成功， 1，失败

DBconnnect_log： 如果连接失败，则记录报错信息。

如果能记录成功信息，也可以考虑记录。



**2， 文件装载结果**

filename1： 装载的文件

result： 入库结果 0 成功，1 失败。

load_log： 如果入库失败，则记录日志。  如果成功，可选择是否记录。

No of record: 如果入库成功，则记录装载的数据条数。



## 9.2     数据库组件简介

​         在前大数据时代，能够掌握关系型数据库，基本上可以胜任90%以上信息化项目，所以在前大数据时代，如果精通一门商用数据库的专业证照，含金量是非常高的。 然而到了大数据时代，因为数据的规模超过了关系型数据库的处理能力，各种分布式的NO-SQL数据库开始流行。 现在掌握一门NO-SQL数据库开始流行。很多刚毕业的同学都认为No-SQL数据库就等于大数据库，所以就忽视了传统关系型数据库的学习。

​         然而从全球的范围来看，关系型数据库的市场收益，远远高于NO-SQL数据库收益。 从市场对关系型数据库的认可程度，说明善用运用关系型数据库，可以极大的节省项目集成的成本。 所以在大数据时代，更要系统的掌握关系型数据库的应用。

![1-ETL 方案](/assets/python-bigdata-starter/cp09/1-ETL 方案.jpg)

​                          图 9-1  经典的数据仓库方案

​       上图是澳洲一家电信公司的在2012年的时候的一个方案。虽然这是一个非常经典的ETL方案，但是在现代的大数据处理项目中，依然可以参考其中的设计思路。作为立志于在大数据领域掘金的你，掌握1门开源的数据库是非常有必要的。而postgres-sql就是一款不错的开源数据库。

![9-2 timeline_postgresql](/assets/python-bigdata-starter/cp09/9-2 timeline_postgresql.png)

​                               图9-2 Postgres-sql 古老而年轻的数据库

### 9.2.1 Postgres-sql数据库

Postgres-sql是一门古老，同时又是一个非常年轻的关系型数据库。PostgreSQL是加州大学伯克利分校计算机系开发的对象关系型数据库管理系统（ORDBMS），具有悠久的历史。PostgreSQL支持大部分 SQL标准并且提供了许多其他现代特性：复杂查询、外键、触发器、视图、事务完整性、MVCC。

![9-3 pg的对象模型](/assets/python-bigdata-starter/cp09/9-3 pg的对象模型.png)

​                    图 9-3 postgres-sql 的基础对象模型与扩展支持

​         而PostgreSQL是一种高度面向对象的数据库，提供强大的扩展能力，可以引入多样化的插件（extensions），PostgreSQL 可以用许多方法扩展，比如， 通过增加新的数据类型、函数、操作符、聚集函数、索引。在最新的PostgreSQL 11 的版本中，原生就可以支持Json 数据库，通过扩展的方式，可以很方便的支持列式存储，空间数据对象，时序数据库（time-scale DB），流数据库（pipeline DB），MPP数据库，利用这些扩展功能，可以很方便的支持一些特殊的大数据分析场景。所以PostgreSQL是一个与时俱进的数据库，可以胜任巨大部分的大数据应用场景。几乎可以实现SQL on everything的开发模式。



![9-4 pg 与大数据](/assets/python-bigdata-starter/cp09/9-4 pg 与大数据.png)



​                    图 9-4 PostgresSQL中数据中心的核心地位

​         利用postgresSQL，你可以很容易的构建一个强大的数据中心。最新的postgres11 提供四大特性来很好的支持数据中心：

1，数据仓库功能：提供一下窗口函数，数据分区，以及bitmap scan特性。

2,NoSQL支撑：可以支持JSON数据类型，数据库分片，以及简化DDL表达。

3，扩展功能：可以支持PostGIS，PL/python等高级分析功能。

4，外部数据库连接器：可以方便的访问多种流行的关系型数据库和NOSQL数据库，

​          从这张图，我们可以发现，掌握了postgres-SQL，其实是一种非常划得来的投资。只要善用他提供的各种新特性和外部插件，就可以很方便的构建一个低成本的数据中心。

### 9.2.2 Postgressql与python融合

  当然PostgreSQL也不是没有短板，直到postgres10之前，都无法支持并行计算的能力，而并行化的特性在使用上还相当的繁琐。所以在一些复杂的处理场景下，处理效率不是很理想，很容易遭遇大数据处理的Velocity（数据需要快速的处理） 陷阱。所以在一些极端的场景下，往往需要将一个处理任务进行并行化转换，并借助python的封装和编程能力，加快数据的处理效率。所以，python的在并行计算方面的强大扩展性，和postgres-sql的丰富的原子能力相结合，就能够提供一种非常强大的低成本解决方案。所以在本实验中，我们将探索如何在python的程序中方便的连接postgres-SQL数据库。这就需要使用相应的数据库连接器Sqlalchemy

### 9.2.3 Sqlalchemy 数据库连接器

​        SQLAlchemy是Python SQL工具包和Object Relational Mapper，它为应用程序开发人员提供了SQL的全部功能和灵活性。它提供了一整套经典的企业级数据持久化模式，旨在实现高效，高性能的数据库访问，并采用简单的Pythonic域语言。

​      传统的程序设计方法有两种，一种是面向对象的程序设计，一种是面向过程的程序设计。传统的SQL 数据库立足于处理更大规模的数据，并提供更好的计算效率，所以并针对对象化封装问题考虑的比较少，也缺乏面向对象的一些特性。 而一些基于内存对象化数据（object collections）处理方案则侧重于处理更多应用场景，所以针对数据抽象和封装方面的内容考虑的比较多，而针对表和数据行方面的处理效率问题则涉及的不多。SQLAlchemy aims to accommodate both of these principles. 而SQLAlchemy则立足于融合这两种设计原则，在兼顾对象化封装的前提下，提供高大规模数据的高效处理。

​       SQLAlchemy认为数据库应该是一种是关系型的科学计算引擎，而不仅仅是各种数据表的集合。行数据不仅可以从表（tables）中选择，还可以从连接（joins）和其他选择语句（select statements）中选择;任何这些数据源头生成的数据单元结构都可以组成一个更大规模的结构。 SQLAlchemy的表达式语言建立在这个概念的核心之上。

​         SQLAlchemy提供非常强大的对象-数据映射功能（object-relational mapper (ORM)），他是一个经过专门优化的组件来实现数据映射的模式，在这种模式下面，对象模型（object model）和数据模型（database schema）可以从一开始就采用一种清晰的解除耦合的模式进行各自独立的开发。这样对象模型可以很方便的转换为数据模型。

![SQLALchemy](/assets/python-bigdata-starter/cp09/SQLALchemy.jpg)

​                        图9-5 SQLAlchemy的组件结构

​        从上图可见，SQLAlchemy是一个基于Python实现的ORM框架。该框架建立在 DB API之上，使用关系对象映射进行数据库操作，简言之便是：将类和对象转换成SQL，然后使用数据API执行SQL并获取执行结果。

 主要的组件说明如下：

·         Engine，框架的引擎

·         Connection Pooling ，数据库连接池

·         Dialect，选择连接数据库的DB API种类

·         Schema/Types，架构和类型

·         SQL Exprression Language，SQL表达式语言

​          SQLAlchemy本身无法操作数据库，其必须依赖psycopg2 ，pymsql等第三方插件，Dialect用于和数据API进行交流，根据配置文件的不同调用不同的数据库API，从而实现对数据库的操作。

参考以下链接：

<https://www.cnblogs.com/wupeiqi/articles/8259356.html>

​       从上面的组件模型，我们可以发现SQLAlchemy's的设计思路和绝大部分现存的SQL / ORM tools并不相同，他是基于一种面向互补性协同设计（complimentarity- oriented approach）的思路。所有的转换过程都是基于一系列可组装，透明转换的工具进行充分透明的处理，而不是像其他的工具一样最大限度隐藏SQL和对象关系之间的细节。SQLAlchemy的组件负责自动执行转换过程中的各类冗余任务，而开发人员仍然可以专注控制数据库的组织方式以及SQL的构建方式。

## 9.3     设计方案

![9-5 object model of agg2db](/assets/python-bigdata-starter/cp09/9-5 object model of agg2db.png)



​                                   图 9-6对象模型

​        从上图可见，本案例通过一个函数就可以轻松的实现从CSV文件到数据库文件的转换处理。从数据转换的角度，主要有三个部分

1，CSV 文件系统： 需要进行聚合的数据主要是在文件系统中进行缓存，并用于后续的处理。

2，DATAframe 组件：在上一章节中介绍过，借助pandas的强大的DF数据结构，我们可以在内存中方便的管理二维数据结构。

3，postgres-sql数据库系统：当数据处理完毕以后，我们就可以从DF中拷贝到数据库当中。



### 9.3.1  Report_df()

![9-6 report dict](/assets/python-bigdata-starter/cp09/9-6 report dict.png)

​                                    图 9-7函数返回的report 字典对象

​        本章的练习是基于第八章《基于pandas的数据报表生成》的后续作业， 虽然在本周的IPO部分，我们假设的是已经经过聚合以后的报表， 直接进行入库操作；但是考虑到复用第八章的内容，所以我们这里假设输入的文件还是原始文件。

所以需要再次使用第八章中设计的report_df() 函数，不过要做一些修改。由于本章我们的目的是要将数据拷贝到数据库当中去，所以生成好df_agg对象以后，我们并不会转换到CSV格式的文件，而是将 他缓存在一个report{} 字典中， 后续直接拷贝到数据库中。

 在report{} 字典中：

  1，key 是汇聚报表的名称；

  2,  value：是生成的df_agg对象

在本案例中，是一个dict，两对key-value。



### 9.3.2  df.to_sql

   当DF生成以后，核心就是将DF拷贝到数据库当中，相关的函数简要解释如下。

`DataFrame.``**to_sql**`(*name*, *con*, *schema=None*, *if_exists='fail'*, *index=True*, *index_label=None*, *chunksize=None*, *dtype=None*, *method=None*)[[source\]](http://github.com/pandas-dev/pandas/blob/v0.24.1/pandas/core/generic.py#L2401-L2532)

Write records stored in a DataFrame to a SQL database.

Databases supported by SQLAlchemy [[1\]](http://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html#r689dfd12abe5-1) are supported. Tables can be newly created, appended to, or overwritten.

在本方案中，使用的语句是：

```
df.to_sql(tablename, con=con, if_exists='append')
```

主要的函数解释如下：

**name** : *string*

Name of SQL table.

**con** : *sqlalchemy.engine.Engine or sqlite3.Connection*

Using SQLAlchemy makes it possible to use any DB supported by that library. Legacy support is provided for sqlite3.Connection objects.

**schema** : *string, optional*

Specify the schema (if database flavor supports this). If None, use default schema.



**if_exists** : *{‘fail’, ‘replace’, ‘append’}, default ‘fail’*

How to behave if the table already exists.

- fail: Raise a ValueError.
- replace: Drop the table      before inserting new values.
- append: Insert new values      to the existing table.

在这几个参数中，比较关键的参数是con:

Using SQLAlchemy makes it possible to use any DB supported by that library

仅仅通过一个参数配置，我们就可以使用SQLAlchemy的强大的数据库连接功能。

相应的语句是：

```
con = create_engine(uri)
```

备注1：首次调用这个组件的时候，假如数据库中没有这个表，组件会自己创建

pd.to_sql 有根据df 创库的功能，入参要求提供 con  （sqlalchemy engine）

if_exists = 'append'   , 还是 if_exists='replace'。  处理有相同表名的情况。



### 9.3.3  容错处理traceback

   在需求部分，有要求记录数据库的连接操作结果，相关的要求说明如下：

1,数据库连接结果

DB_connect result： 0 成功， 1，失败

DBconnnect_log： 如果连接失败，则记录报错信息。

如果能记录成功信息，也可以考虑记录。

   由于数据库是一个外部组件，连接数据库可能成功，也可能失败，所以我们使用以下的语句。

![9-7 入库操作](/assets/python-bigdata-starter/cp09/9-7 入库操作.png)



## 9.4     处理结果

   在本方案中，使用了pandas强大的df.to_Sql函数，轻松的将内存中的数据对象，传递到数据库当中，这样我们不需要理解复杂的数据库操作，就可以轻松的收获数据库强大的红利。



   相关的打印日志见下图：

![9-8 入库操作日志](/assets/python-bigdata-starter/cp09/9-8 入库操作日志.png)



## 9.5     方案总结

  在本方案中，我们实现了将文件到数据库表的转换，打开了一扇通向大数据处理的大门，主要的概念总结如下：

1，postgre-SQL是一个古老而年轻的开源数据库组件，适合构建低成本的大数据持久化系统。

2，SQLalchemy是一个创新型的ORM组件，他兼顾了对象化处理的高度抽象能力，以及数据库系统强大的处理效率，搭建了内存系统和数据库系统的桥梁。

3，pandas的DF可以很方便的利用SQLalchemy的能力，轻松实现数据的入库操作。
