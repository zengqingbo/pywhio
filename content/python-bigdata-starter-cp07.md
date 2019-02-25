---
title: "Python BigData Starter CP07"
cover: "7.jpg"
category: "python"
date: "2019-02-25"
slug: "python-bigdata-starter-cp07"
tags:
---

​    俗话说“温故而知新”，流转换的基础用例已经分享完毕，今天将总结一下，如何从csv文件生成xml文件，这样以后聪明的你也将知道如何利用csv文件生成xml文件。





# 7  附录：如何实现从csv到xml转换

在第四章的实验中，我们分享了如何将xml文件转换为csv件，

理解了使用oo方法来构建xml解析器的设计思路。 然而读者已经理解了xml解析的巧妙，可能会想如何将csv转换为xml呢？ 这显然也是一个比较有群的问题，然而这个问题I需要的背景知识已经远远超越了python big data stater 的范围，所以我们考虑采用附录的方式来记载相关的设计方案。



## 7.1   背景介绍

## 7.1.1  lxml 组件

​     The lxml XML toolkit is a Pythonic binding for the C libraries [libxml2](http://xmlsoft.org/) and [libxslt](http://xmlsoft.org/XSLT/). It is unique in that it combines the speed and XML feature completeness of these libraries with the simplicity of a native Python API, mostly compatible but superior to the well-known [ElementTree](http://effbot.org/zone/element-index.htm) API. The latest release works with all CPython versions from 2.7 to 3.7. See the [introduction](https://lxml.de/intro.html) for more information about background and goals of the lxml project.

​       

​       组件lxml是一个透过python语言封装了基于C语言编程的**libxml2** and **libxslt**插件的.XML解析器。Lxml设计的独一无二之处在于，他在内部处理性能方面利用C语言强大的效率优势保障了xml文件处理的效率，同时在外部接口方面提供了完整的易于调用调用接口，具备了一个python原生API的全部特性，这样能够使用 [ElementTree](http://effbot.org/zone/element-index.htm) API的读者也可以非常方便的使用Lxml组件。另外，除了兼容ElemenTree的功能以外，lxml还能够提供更多强大的功能，支持一些个性化的功能。



## 7.2 XML结构构造

   接下来，我们看看期望生成的XML文件的结构说明。

![7-1 XML 组成结构](/assets/python-bigdata-starter/cp07/7-1 XML 组成结构.png)

​                        图7-1 XML 组成结构



​        以上是相关的XML文件的组成结构。其中主要包含了5个部分。

1，XML 声明： 是每个xml文件都需要包含的通用声明部分。

2，XML namespace： 是一个公共说明部分。

3,File header 用于说明ticket file 的公共说明部分。

4，ticket name： 用于表达CSV文件的表头部分的信息结构。

 5，ticket data： 用于表达CSV文件的行数据部分的信息结构。

  下面简要的看看，各个部分是如何生成的。



### 7.2.1  创建Name Space 部分的代码

NS = 'http://www.w3.org/2001/XMLSchema-instance'

location_attribute = '{%s}noNameSpaceSchemaLocation' % NS

  TicketFile = ET.Element("TicketFile", attrib={location_attribute: 'TicketFileFormat.xsd'})

​       在整个XML的创建部分，**生成namespace 部分的代码是最难以理解的**。

相关的内容可以参考以下的参考资料。

https://stackoverflow.com/questions/863183/python-adding-namespaces-in-lxml

![7-2 XML name SPACE](/assets/python-bigdata-starter/cp07/7-2 XML name SPACE.png)

核心的NS 配置代码

```
>>> location_attribute = '{%s}noNameSpaceSchemaLocation' % NS
>>> elem = etree.Element('TreeInventory', attrib={location_attribute: 'Trees.xsd'})
>>> etree.tostring(elem, pretty_print=True)
'<TreeInventory xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="Trees.xsd"/>\n'
```



经过这一段代码，就可以创建出期望的NS结构：

```
<TreeInventory xsi:noNamespaceSchemaLocation="Trees.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
</TreeInventory>
```



### 7.2.2  构造 FileHeader

File header 是1个XML文件中需要的一个配置信息， 本案例中的信息结构，是从一个外部的excel 文件中导入的，信息结构见下图：

![7-3 XML header](/assets/python-bigdata-starter/cp07/7-3 XML header.png)

利用pandas组件，会读取相关的信息结构，读取的语句为：

```
fileheader = pd.read_excel("./CP4- header 规划.xlsx",index_col="文件名").to_dict("index")
```

利用这个语句，会生成一个dict，相关的结构是

Fileheader={‘QLR2019020710-001.csv’:{‘informationmodelreference’：’bank-ticket-v1.0’,  ‘sitename’:’QLR’, ‘begintime’:201802071002,’endtime’=201802071004’     },

 {‘QLR2019020710-002.csv’:{‘informationmodelreference’：’bank-ticket-v1.1’,  ‘sitename’:’QLR’, ‘begintime’:201802071006,’endtime’=201802071008’ } , {‘QLR2019020710-003.csv’:{‘informationmodelreference’：’bank-ticket-v1.2’,  ‘sitename’:’QLR’, ‘begintime’:201802071010,’endtime’=201802071011’ }}

相关的代码阐述如下：

   FileHeader = ET.SubElement(TicketFile, "FileHeader")

   for k,v in header.items():

​      i = ET.SubElement(FileHeader, k )

​      i.text = str(v)

​       大致的过程，描述如下

  1，利用函数ET.SubElement(TicketFile, "FileHeader") ，在根节点TicketFile下创建一个子节点FileHeader。

  2，针对自动header中的1个元素，进行遍历操作。

  3， 设定i为节点FileHeader 的子标签， header有四个子标签， 其中key为字段名， value是字段数值。

   这里使用了 for in 的方法， 这里 Dict fileheader 的items 就是k， V。

   Fileheader是ticketfile 的第一个标签

### 7.2.3  构造measurement

​        接下来创建Measurement， 也就是记录我们的银行交易清单的数值部分。  其中measurement 就是负责表达一个CSV文件的表头部分和信息结构部分。

​        在这里measurement 是ticketfile 的第二个标签。

  Measurement 下面由很多的标签。

 1，第一个标签是objecttype，这个是用来声明，当前的measurement是一个什么样的object type。他是一个ticket。

 2，第二个标签是ticketname，是字段名。他是用/N标签来标记。

3，第三个标签是ticketdata， 是字段值。他是/V 标签来标记。

### 7.2.4 构造 ticket name

   下面我们讲解一下，如何创建相关的ticket name 标签，主要的代码说明如下：

​    TicketName =  ET.SubElement(Measurements, "TicketName")

​    csv_rows = csv_reader(filename)

​    **for k,v in enumerate(csv_rows[0] , 1) :**

​        i = ET.SubElement(TicketName, "N" )

​        i.set("i", str(k))

​        i.text = v



现在创建的是tickname标签，在这里ticketname是用/N 标签来保存的。见这个语句i = ET.SubElement(TicketName, "N" )，也就是在Tickname这个标签的下一级增加/N 标签的方式，也就是循环添加。

   那具体要添加多少个/N 标签呢，csv_rows[0]，也就是CSV的第0行，编号从1开始，所有的字段都要添加/N标签。

   这里使用了一个enumerate(csv_rows[0] , 1) 函数，他是一个很有特点的迭代函数，他可以返回序号。因为这里我们想从list中取出每一个元素，一般会使用 for i in list[],  他会取list[] 中的每一项给i, 执行一个循环操作，但是他没有标号，程序没法知道取得元素的标号，也就是第几个，所以使用enumerate 函数，这样应用程序可以取得元素，同时得到元素的标号，这里标号就是第一个参数K，这样就可以知道N标签中的i属性，也就是第几个就能够知道，也就是后续用于为i属性进行赋值，也就是标号1，2,3,4 等。

![7-4 生成ticket name](/assets/python-bigdata-starter/cp07/7-4 生成ticket name.png)

   经过赋值操作以后，就可以很方便的生成ticket name。

注： 这里因为标签中有中文，所以只能通过value的方式来进行表达，也就是通过字符串的方式来表达。

### 7.2.5 构造tickt data

接下来构造数据，也就是从CSV的第二行开始。 Ticket 的结构见下图：

![4-11XML 树形结构20190215](/assets/python-bigdata-starter/cp07/4-11XML 树形结构20190215.png)



​                           图 7-4 ticket data 的结构

​        此处，由于ticket data 对应的CSV文件，是有多行的，由于并没有配置每一行的信息，所以在这里我们姑且通过id来进行表达，这里id来表达序号。也就是属性。

  主要的代码说如下：

TicketData = ET.SubElement(Measurements, "TicketData")

**for k,v in enumerate(csv_rows[1:] , 1) :**

   Ticket = ET.SubElement(TicketData, "Ticket" )

   Ticket.set("Id", str(k))

   **for l, w in enumerate(v , 1) :**

​      i = ET.SubElement(Ticket, "V" )

​      i.set("i", str(l) )

​      i.text = w

主要的过程说明如下：

1，首先在节点TicketData下创建一个Ticket标签。这里1笔记录是一个ticket。

2，对for k,v in enumerate(csv_rows[1:] , 1) ，对list进行循环，这里k是CSV文件的行数，v 是行数据的内容。 这个是第一层的循环。

3，针对每一行数据，创建1个ticket标签。

4,还有第二层循环for l, w in enumerate(v , 1)，这里l是1行数据的列号，w是其中的单元格的内容。 因为有两层循环，所以用l，w。

5，开始创建V标签， 根据每一行的内容，创建多个子标签，其中的序号，来自enumerate(v , 1)中返回的l， 而对应的值来自其中返回的w。

​        ![7-5 生成ticket data](/assets/python-bigdata-starter/cp07/7-5 生成ticket data.png)

​     经过上面的操作，就可以生成相关的树形结构。



### 7.2.6  打印XML文件

​       相关的语句是：

```
ET.ElementTree(TicketFile).write(filename_xml, encoding='UTF-8', xml_declaration=True, method='xml', pretty_print = True)
```



​          这里选择了一个参数是**pretty_print = True**，这个是组件Lxml组件提供的特有的打印功能，可以生成可读形式的xml文件。



## 7.3     总结

​          在 本章节中，我们补充了XML文件的生成相关的知识。总结起来。

1，我们了解了lxml组件的使用，他涵盖了ElementTree的功能，还提供了其他更加强大的功能，比如pretty_print = True。

2，在xml的构建过程中，我们进一步理解了python中利用list[]来实现循环的操作。

3，对xml的标签机制有可更加深入的理解。
