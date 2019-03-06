---
title: "Python BigData Starter CP10 plus"
cover: "3.jpg"
category: "python"
date: "2019-03-06"
slug: "python-bigdata-starter-cp10-plus"
tags:
---​

## 10.8 SQLAlchemy  应用讨论

​          使用SQL方式进行数据的汇总操作有一个巨大的优势是方便，

 快捷，但是使用SQL方式存在一个很大的缺陷就是当查询条件发生变更时需要手工修改相关的语句，难以理解，也难以维护。从软件复用的角度开看，这是SQL方式一个很大的不足。

​            而SQLALchemy 则在此基础上做出改进， 他结合SQL语言的语法规则进行了对象化封装，使得SQL语句可以在不赖具体数据库的情况下，进行一种对象化的表达，进而获得一种重用的特性。

​        所以我们针对数据库汇聚函数进行补充讲解。



### 10.8.1 SQL 操作基础

   要理解SQL语句，首先要理解SQL的表达方式：

**1，SQL DML 和 DDL**

 可以把 SQL 分为两个部分：数据操作语言 (DML) 和 数据定义语言 (DDL)。

SQL (结构化查询语言)是用于执行查询的语法。但是 SQL 语言也包含用于更新、插入和删除记录的语法。

查询和更新指令构成了 SQL 的 DML 部分：

·         ***SELECT*** - 从数据库表中获取数据

·         ***UPDATE*** - 更新数据库表中的数据

·         ***DELETE*** - 从数据库表中删除数据

·         ***INSERT INTO*** - 向数据库表中插入数据

SQL 的数据定义语言 (DDL) 部分使我们有能力创建或删除表格。我们也可以定义索引（键），规定表之间的链接，以及施加表间的约束。

SQL 中最重要的 DDL 语句:

·         ***CREATE DATABASE*** - 创建新数据库

·         ***ALTER DATABASE*** - 修改数据库

·         ***CREATE TABLE*** - 创建新表

·         ***ALTER TABLE*** - 变更（改变）数据库表

·         ***DROP TABLE*** - 删除表

·         ***CREATE INDEX*** - 创建索引（搜索键）

·         ***DROP INDEX*** - 删除索引



  **2， SQL GROUP BY 语句**

​    由于本项目中主要使用分组-聚合操作，相关的语法结构主要描述如下：

  SQL GROUP BY 语法

```
SELECT column_name, aggregate_function(column_name)
FROM table_name
WHERE column_name operator value
GROUP BY column_name
```



  结合上图可见本项目中要拼装出类似上面的SQL语句。



### 10.8.2 对象化方法生成SQL语句

​          SQL Expression Language 是SQLAlchemy Core的组成部分, 提供了与SQL类似的API而避免了直接书写SQL语句.

  在本项目的函数中，期望生成的SQL 语句是：

![select 对象的链式操作](/assets/python-bigdata-starter/cp10/select 对象的链式操作.png)

​     对象化生产SQL语句的方法见下图：

![10plus-1 SQL对象创建](/assets/python-bigdata-starter/cp10/10plus-1 SQL对象创建.png)

​                                            图 10-5  对象化生成SQL语句的过程

  从上面图可见，创建语句的过程总共分为三步。

1， **生成select对象**： 根据用例分析，我们首先需要执行分组-汇聚操作，生成一个结果集合，这可以通过1个select对象来实现相关的表达式。

2,  **生成期待入库的table对象**：结果集需要拷贝到1个外部数据库的表中，所以需要创建1个表对象，来表达这个表结构。

3, **生成入库操作的insert对象**：将期待拷贝的表对象，和用于生成结果集的select对象进行粘合，就能够生成insert对象。



### 10.8.3 从面向对象看SQLALchemy

![10plus-2 现实 模型世界](/assets/python-bigdata-starter/cp10/10plus-2 现实 模型世界.png)

​                                              图 10-6 从OO看SQLALchemy

​           从上图可见，假如我们可以看出SQLALchemy解决数据库连接问题的思路。相关的领域问题可以分为三个领域。

1，**现实世界**： 常规的数据库应用的时候，我们都是通过SQL语句来操作数据库，此时SQL语句一般是通过界面输入，或者是事先编辑好，在通过工具提交。

2， **模型世界**：而SQLALchemy提供了SQL Expression Language的能力，这种能力屏蔽了现实世界的各种DB的实现上的差异，这从SQL构成要交的角度，构成可一个SQL语句的模型。根据DML的不同，可以通过不同的select对象，和insert对象，以及和insert对象关联的table对象。

   而利用python语言强大的模型表达能力，我们可以不必查询真实的数据库就构建一个模型世界的表结构。

3，**DB连接器**： 假如我们想最后希望实现 现实世界的操作，SQLALchemy提供的DB连接器，会根据现实世界的DB类型，提供适配能力，这样应用程序无需关心不同DB的差异。而专注于构造正确的SQL模板。



### 10.8.4 Select对象的链式调用

​       由于执行分组-汇聚操作的select语句比较复杂，所以select提供了一种链式调用能力，可以不断的自己调用自己，每次调用使用不同的方法。 这也可以成为是一种反身调用。 相关的意涵见下图。

​      ![10plus-3 链式调用](/assets/python-bigdata-starter/cp10/10plus-3 链式调用.png)

​                       图 10-7 链式调用

​       链式调用的好处是可以非常清晰的表达程序的逻辑关系，这种模式node.js  和 python 里面随处可见。这种特性是低成本开发的保障。

   下面的语句也是一个这样的例子：

```
ins = t.insert().from_select(sel.columns, sel, False)
```



## 10.9     SQL语句渲染

 为了生成一些复杂的SQL语句模型，我们可以引入下列包，实现SQL语句的渲染功能 （render）：

```
from sqlalchemy.sql import table, column ,literal_column, func
```



### 10.9.1 sqlalchemy中列元素和表达式

sqlalchemy.sql.expression模块下用于构成类SQL表达式的元素

1. sqlalchemy.sql.expression.**table**(*name*, **columns*)

Produce a new [TableClause](https://docs.sqlalchemy.org/en/latest/core/selectable.html?highlight=table#sqlalchemy.sql.expression.TableClause).

The object returned is an instance of [TableClause](https://docs.sqlalchemy.org/en/latest/core/selectable.html?highlight=table#sqlalchemy.sql.expression.TableClause), which represents the “syntactical” portion of the schema-level [Table](https://docs.sqlalchemy.org/en/latest/core/metadata.html#sqlalchemy.schema.Table) object. It may be used to construct lightweight table constructs.

2. sqlalchemy.sql.expression.**column**(*text*, *type_=None*, *is_literal=False*, *_selectable=None*)

Produce a [ColumnClause](https://docs.sqlalchemy.org/en/latest/core/sqlelement.html#sqlalchemy.sql.expression.ColumnClause) object.

The [ColumnClause](https://docs.sqlalchemy.org/en/latest/core/sqlelement.html#sqlalchemy.sql.expression.ColumnClause) is a lightweight analogue to the [Column](https://docs.sqlalchemy.org/en/latest/core/metadata.html#sqlalchemy.schema.Column) class. The [column()](https://docs.sqlalchemy.org/en/latest/core/sqlelement.html#sqlalchemy.sql.expression.column) function can be invoked with just a name alone, as in:

3. sqlalchemy.sql.expression.**literal_column**(*text*, *type_=None*)

Produce a [ColumnClause](https://docs.sqlalchemy.org/en/latest/core/sqlelement.html#sqlalchemy.sql.expression.ColumnClause) object that has the [column.is_literal](https://docs.sqlalchemy.org/en/latest/core/selectable.html#sqlalchemy.sql.expression.Select.column.params.is_literal) flag set to True.

[literal_column()](https://docs.sqlalchemy.org/en/latest/core/sqlelement.html#sqlalchemy.sql.expression.literal_column) is similar to [column()](https://docs.sqlalchemy.org/en/latest/core/sqlelement.html#sqlalchemy.sql.expression.column), except that it is more often used as a “standalone” column expression that renders exactly as stated; while [column()](https://docs.sqlalchemy.org/en/latest/core/sqlelement.html#sqlalchemy.sql.expression.column) stores a string name that will be assumed to be part of a table and may be quoted as such, [literal_column()](https://docs.sqlalchemy.org/en/latest/core/sqlelement.html#sqlalchemy.sql.expression.literal_column) can be that, or any other arbitrary column-oriented expression.

4. sqlalchemy.sql.expression.**func** *= <sqlalchemy.sql.functions._FunctionGenerator object>*

Generate [Function](https://docs.sqlalchemy.org/en/latest/core/functions.html#sqlalchemy.sql.functions.Function) objects based on getattr calls.



### 10.2.2 SQL语句渲染案例

示例用如下方式从数据库查询操作，而非通过执行SQL语句

![img](file:///C:/Users/zhanying/AppData/Local/Temp/msohtmlclip1/01/clip_image010.jpg)



## 10.10     Dateutil

  由于本项目使用了天粒度的汇总，所以使用一下的语句：

```
from dateutil.parser import parse
```

   dateutil.parser 顾名思意 就是与日期相关库里的一个日期解析器 能够将字符串 转换为日期格式。

在主函数中使用了一下的操作：  

```
  agg_many_tables({'re_site':'re_site_d','re_customer':'re_customer_d' },
                  list(map(parse, ['201802071000+8','201802071100+8'])),
                  agg_table_sql
                   )
```

备注：此处map的意涵是parse方法要针对list['201802071000+8','201802071100+8'] 中的每个元素执行相关的方法。

最后的处理结果非常完美：

 ![10-A3-chapter10 result](/assets/python-bigdata-starter/cp10/10-A3-chapter10 result.jpg)
