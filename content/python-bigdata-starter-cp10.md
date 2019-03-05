---
title: "Python BigData Starter CP10"
cover: "2.jpg"
category: "python"
date: "2019-03-04"
slug: "python-bigdata-starter-cp10"
tags:
---​

​      在第九章掌握了数据库的python操作方案以后，我们可以更加方便的实现更大数据范围内的数据聚合操作。这样我们可以见证python+postgressql双剑合璧的威力。



# chapter10 两种方法实现天粒度汇总

​          当我们将小时粒度的数据都已经装载到数据库以后，我们后续可以每个小时启动1次数据装载的操作，当我们成功装载了24次数据以后，我们可以进行天粒度的汇总操作。

   在这个章节中，我们将演示两种汇总方法

1，          数据库方案（python调度）

2，          Pandas 方案



# 10.1     数据库结构说明

1，源表

 re-site   ： 站点级小时粒度汇总表名

re-customer：客户级 小时粒度汇总表名

源表按照天粒度分片。

2、目标表

 Re-site-day：站点级天粒度汇总表名

 Re-customer-day：客户天粒度汇总表名

目标表不分片。

3，时间粒度

天粒度汇总表和小时粒度汇总表的区别在与，timescale字段，

分别是：天粒度：timescale _date

​        小时粒度：timescale



## 10.2     Pandas方案需求分析

### 10.2.1 Pandas方案用例回顾

1，          用户输入汇总操作参数，包含汇总的数据库表名称， 期望汇总的日期。

2，          应用程序根据数据数据，生出一个job list

3，          应用程序根据数据库表名称，和期望能够汇总的日期，筛选相关的数据，并传入内存中，生出一个DF。

4，          应用程序对DF进行汇总操作，生出DF_agg

5，          应用程序将DF_AGG 拷贝到数据库当中。

6，          应用程序将所有期待汇总的表顺序完成汇总操作。

### 10.2.2 输入数据（I）

​       输入数据的情况如下，[ Table_pair], [date]，[re_day_cfg], pg_conn_dict

下面将相关的输入数据进行阐述：

1、Table_pair=[[source table1, dest table1], [source table2, dest table2]]

2、Date=[date1,date2]

3、Re_day_cfg:  聚合算法配置

![10-A 配置文件](/assets/python-bigdata-starter/cp10/10-A 配置文件.png)

   相关配置报表说明如下。



4、pg_conn_dict

   配置数据库的连接信息：

pg_conn_dict = {

​     "host": "10.x.x.x",

​     "dbname": "bank",

​     "user":"postgres",

​     "password":"root"

}



### 10.2.3 处理过程（P）

1，          应用程序读取输入数据，根据[ Table_pair], [date], [re_day_cfg] 生成job list。

2，          应用程序根据job list ，启动1个任务的处理。

3，          应用程序从源表中读取需求的数据，装载到内存中形成DF1.

4，          应用程序根据[re_day_cfg]的信息，进行聚合操作，生成1个DF2.

5，          应用程序将DF2 导入到目标表中.

6，          将其他的天粒度汇总任务处理完毕。

7，          释放数据库连接。

8，          反馈结果。



### 10.2.4 输出数据（o）

   输出的结果是[ DB_connect result, DBconnnect_log],

 [[ source table1,date1, result, load_log, No of record],]

输出结果包含两个要素：



1, 数据库连接结果

DB_connect result： 0 成功， 1，失败

DBconnnect_log： 如果连接失败，则记录报错信息。

如果能记录成功信息，也可以考虑记录。



2、天粒度汇总结果

filename1： 装载的文件

result： 入库结果 0 成功，1 失败。

load_log： 如果入库失败，则记录日志。  如果成功，可选择是否记录。

No of record: 如果入库成功，则记录装载的数据条数。



## 10.3     数据库方案需求分析



### 10.3.1 数据库方案用例回顾

1、用户输入汇总操作参数，包含汇总的数据库表名称， 期望汇总的日期。

2、应用程序根据输入数据数据，生成1个joblist

3，应用程序启动1个job，根据相关的参数，生成一个数据库处理脚本。

4，应用程序启动数据库脚本，完成汇总操作，并将汇总表入库处理。

5，应用程序将所有期待汇总的表顺序完成汇总操作。

### 10.3.2 输入输出数据（I/o）

​       同pandas方案

### 10.3.3 处理过程（P）

1，          应用程序读取输入数据，根据[ Table_pair], [date], [re_day_cfg] 生成job list。

2，          应用程序选择1个job，根据配置数据，生成1个sql脚本。

3，          应用程序执行SQL脚本，并将结果入库。

4，          将其他的天粒度的汇总数据处理完毕。

5，          释放数据库连接。

6，          反馈结果。

## 10.4     总体设计方案

​        由于整个处理是的输入，输出是相同的，能否利用对象封装，提供两个方法，一个是利用pandas，一个是利用数据库脚本。下面我们就来看看，相关的设计方法。

### 10.4.1 总体设计方案

   ![10-1 总体组件对象](/assets/python-bigdata-starter/cp10/10-1 总体组件对象.png)

​                                     10- 1 总体对象模型

​      从上图可见，本方案中的结构比较简单。主要有三大部分组成

1，主函数agg_many_tables():  用于调用函数实现数据表的按日期汇总功能。

2，getcon() : 用于和postgres数据库建立连接，并维护相关的连接。

3，agg_table_fun: 函数对象。应用接收函数传参。



### 10.4.2 函数作为入参对象

 由于本方案中，提供两种实现方案， 所以我们在主函数中，通过传参的方式确定具体调用的函数来实现数据的聚合功能。

  ![10-A2 函数传参对象](/assets/python-bigdata-starter/cp10/10-A2 函数传参对象.png)

使用两种方法都可以获得相同的结果，相关的调试日志如下：

​     ![10-A3-chapter10 result](/assets/python-bigdata-starter/cp10/10-A3-chapter10 result.jpg)



## 10.5     Pandas 汇聚函数

   ![10-2 pandas报表生成](/assets/python-bigdata-starter/cp10/10-2 pandas报表生成.png)

​                       图 10-2 pandas aggregator

   从上图可见，相关的pandas 汇聚函数的对象试图简要说明如下。

1，          数据装载：使用pd.read_sql_table() 将数据从数据库当中拷贝到内存中，生成1个DF对象。

2，          数据汇聚： 利用pandas的DF提供的函数，进行分组-聚合操作，生出1个DF_agg对象。

3，          数据输出：利用DF_agg对象。提供的df_agg.to_sql() 方法，将数据拷贝到数据库当中。

从上图可见，相关的处理过程比较直接，只需要专注于数据流转换的过程就可以。

由于pandas强大的基础封装能力，我们可以非常方便的把数据

从数据库中拷贝到内存中，在内存中进行弹性的处理，最后在把结果集回传给数据库。

### 10.5.1 分组聚合操作

  ![10-3 groupby function](/assets/python-bigdata-starter/cp10/10-3 groupby function.png)

​                      图 10-3 分组-聚合函数

下面将以站点级别的分组-聚合操作予以说明。

![10-A4分组 汇总处理](/assets/python-bigdata-starter/cp10/10-A4分组 汇总处理.png)

  从上面的操作过程可见，经过聚合操作以后，生成了一个新的DF，其中就包含了我们期待获得的汇聚表。从上面的分组汇总函数中，我们采用的是任意时间段的函数汇总，如果后续想实现天粒度，周粒度，月粒度汇总的时候。在进行封装控制。

```
 备注： 在原型项目中这里提到了date1,date2  就按[date1, date2)时间范围汇总了，后续如果要实现天粒度无非就是入参控制一下就可以。

在本案例中，赋值方案如下：
df_agg[time_field] = date[0]  也就是在df_agg增加1个time_field字段。
```



### 10.5.2 异常记录

​        由于相关的数据库入库操作可能成功，也可能失败，所以目前考虑要对相关的日志进行记录，相关的代码简单说明如下：

​    log = 'table:%s , insert records:%s' % (dest_table, len(df_agg))

​    try:

​        df_agg.to_sql(dest_table, con=con, if_exists='append')

​    except:

​        log = traceback.format_exc()

​        return [1,dest_table,log]

print(Fore.BLUE + log + Style.RESET_ALL)



在相关的处理中：

1，          首先生出一个log的临时变量，用于记录入库的目标表，和期望入库的记录数。

2，          接下来尝试连接数据库，进行数据插库操作。

3，          如果发现异常，则调用traceback.format_exc()记录一场日志，并返回报错信息。

4，          如果一切正常，则返回入库成功信息。

5，          打印返回结果集。

### 10.5.3 df sql预过滤

​         在常规方案中，数据从数据库装载到内存中，使用的是如下的语句：

```
df = pd.read_sql_table(source_table,con)

df = df[(df[time_field]>=date[0]) & (df[time_field]<date[1])]
```

​      相关的思路是首先将数据表的内容读入DF， 在将DF中进行时间过滤，获取期望聚合时间段的数据。在第一步的操作中，如果数据表的数据量很大，全部加载在内存中，这可能会导致内存超限的问题，存在一定的风险。

要考虑采用稳健的处理方法，也就是先过滤再将数据从数据库拷贝到内存中。

```
df = pd.read_sql(sql,con)

sql = "select * from {0} where {1}>='{2:%Y-%m-%d %H:%M}' and {1}<'{3:%Y-%m-%d %H:%M}'".format(source_table,column(time_field),*date)

df = pd.read_sql(sql,con)
```

从上面的语句可以看出拼装SQL的方案中，使用了

'{0},{1}'.format(var0,var1)  以下的传参格式。

**备注1：  *[date1,date2]   equal  date1, date2**



## 10.6     数据库汇聚函数

​          虽然pandas汇聚方案是当前流行的方案，然而使用数据库档案也是一种非常经典的方案。在本章的练习中，我们将立即如何利用SQLAlchemy，将数据库的一些处理过程，提供一定的抽象性，这样可以构造一些更加易于变化的组件。

### 10.6.1 抽象使用复杂SQL

​         Using More Specific Text with table(), literal_column(), andcolumn()

在SQLAlchemy中通过table()、literal_column()和column()使用更加复杂的SQL

（备注2：前面有介绍相关text() 的应用场景，text()可以用在select(…).where(…).select_from中，通过输入表名、条件，甚至是完整的sql直接去执行，而不用使用类似于Table()、Column()等MetaData对象。

但是，这种方法其实是绕过了SQLAlchemy的抽象，和直接使用sql类似，笔者是建议除非迫不得已sql非常特殊，可以使用这种方法，一般情况下还是遵循SQLAlchemy Core的抽象较为合理，代码在不同数据库之间的可移植性也较强。SQLAlchemy的设计是较为合理的，较好的兼顾了不同层次的抽象要求，所以下面这段原文其实是给出了一个更好的折衷用法）



​     We can move our level of structure back in the other direction too, by using column(), literal_column(), and table() for some of the key elements of our statement.  Using these constructs, we can get some more expression capabilities than if we used text() directly, as they provide to the Core more information about how the strings they store are to be used, but still without the need to get into full Table based metadata. Below, we also specify the String datatype for two of the key literal_column() objects, so that the string-specific concatenation operator becomes available. We also use literal_column() in order to use table-qualified expressions, e.g. users.fullname, that will be rendered as is; using column() implies an individual column name that may be quoted:



​       我们可以通过对我们sql语句的关键要素使用column()、literal_column()、table()等函数，使我们的sql整体结构向更好更合理的抽象层次发展。由于我们向SQLAlchemy Core提供了更多关于字符串所指的对象如何使用的信息，相对于直接使用text()，通过使用以上这些函数，我们能够得到更多的表达式功能，同时，也不需要完全使用基于metadata的Table()等对象的抽象。下面的代码中，我们对两个关键字字段的literal_column()对象，指定了字符串（String）数据类型，这样的话就可以使用python中的字符串连接运算符（+）。另外，我们为了指定特定表中的字段，也使用literal_column()对象，例如：users.fullname，这个在后续解析执行中，会保留users前缀；而使用column()则意味着一个可以直接引用的单独列名。

参考 https://blog.csdn.net/heminhao/article/details/73603388

### 10.6.2 SQLAlchemy的几个函数

​         SQLAlchemy认为数据库应该是一种是关系型的科学计算引擎，而不仅仅是各种数据表的集合。行数据不仅可以从表（tables）中选择，还可以从连接（joins）和其他选择语句（select statements）中选择;任何这些数据源头生成的数据单元结构都可以组成一个更大规模的结构。 SQLAlchemy的表达式语言建立在这个概念的核心之上

参考  https://www.jianshu.com/p/0ad18fdd7eed

\1. Table类

构造函数：

```
Table.__init__(self, name, metadata,*args, **kwargs)
参数说明：
```

1）name 表名
 2）metadata 共享的元数据
 3）*args Column 是列定义，详见下一节
 4）下面是可变参数 **kwargs 定义
 schema 此表的结构名称，默认None
 autoload 自动从现有表中读入表结构，默认False
 autoload_with 从其他engine读取结构，默认None

\2. Column类

构造函数：

```
Column.__init__(self,  name,  type_,  *args,  **kwargs)
参数说明
```

1)、name 列名
 2)、type_ 类型，更多类型 sqlalchemy.types
 3)、*args Constraint（约束）, ForeignKey（外键）, ColumnDefault（默认）, Sequenceobjects（序列）定义
 4)、key 列名的别名，默认None


 下面是可变参数 **kwargs
 5)、primary_key 如果为True，则是主键
 6)、nullable 是否可为Null，默认是True
 7)、default 默认值，默认是None
 8)、index 是否是索引，默认是True
 9)、unique 是否唯一键，默认是False



### 10.6.3 对象模型

![10-Y 对象模型](/assets/python-bigdata-starter/cp10/10-Y 对象模型.png)

​                                     10-4  基于数据库聚合函数的对象模型

​          从上图，可见，核心的处理思想是通过抽象的方式来创建SQL字符串，并通过传参的方式，执行SQL语句。

  拼装的SQL 语句见下图。

   ![10-A5 字符串拼装](/assets/python-bigdata-starter/cp10/10-A5 字符串拼装.png)





## 10.7       讨论

​        从本案例中，我们可以掌握

1， 利用pandas实现很有弹性的数据汇总操作，实现从data-information的处理。

2，我们也演示了数据库的方案，为了提升复用特性，我们利用SQLAlchemy的函数实现了弹性的数据库操作方案。

3，另外我们也演示了如何将函数进行传参的操作。
