---
title: "Python BigData Starter CP04"
cover: "8.jpg"
category: "python"
date: "2019-02-14"
slug: "python-bigdata-starter-cp04"
tags:
---

​         XML 文件是一种具有自描述特性的文件，非常适合表达一些复杂的信息结构，所以也是一种非常重要的文件格式。如何将XML文件转换为方便阅读的CSV格式，并从中提取需要的信息结构， 是非常重要的一种数据处理技能。





# 4 XML文件解析

## 4.1 问题背景分析

​       XML 文件是一种具有自描述特性的文件，非常适合表达一些复杂的信息结构，所以也是一种非常重要的文件格式。如何将XML文件转换为方便阅读的CSV格式，并从中提取需要的信息结构， 是非常重要的一种数据处理技能。



### 4.1.1 输入数据分析

在这里，我们假设银行的交易记录是XML格式的数据，相关的组成结构见下图所显示：

![XML ticket](/assets/python-bigdata-starter/cp04/XML ticket.jpg)

​                                             图 4-1 XML 格式的交易清单的组成结构

​          从上图可见， 相关的XML的数据包含以下的组成部分。

1， XML 的header 部分： 包含信息模型参考索引， 产生xml文件的营业网点名称， XML中包含的交易清单的开始时间和结束时间。

2，交易清单的表头部分（信息结构的名称）: 描述交易ticket的全部信息结构的名称，以list列表的方式来呈现（信息结构的名称在”value”部分进行表达）。

3，交易清单的数据部分（休息结构的取值）：描述交易ticket的全部信息结构的取值，以list列表的方式来呈现（信息结构的取值在“value”部分进行表达）。



根据上面的格式，我们可以提供三个xml 文件。

文件的命名为：

QLR2019020710-001.xml

QLR2019020710-002.xml

QLR2019020710-003.xml

关于输入数据的描述，参考以下文件。



### 4.1.2 XML 解析处理过程（P）

​      1，用户界面向处理组件传递参数，入参包括 需要 进行xml解析的文件清单，以及期望的解析后的CSV文件清单。

2，处理组件接收入参以后， 首先根据文件清单中的文件绝对路径，通过文件系统，识别需要处理的文件列表。

3、处理组件读取一个配置文件， ticket_infofield.py 配置文件，确定需要提取的信息结构。

3，处理组件提取第一个文件，根据文件的后缀名称确定为xml格式，就调用xml处理组件，首先识别出xml文件的ticket_name部分的信息结构取值，将相关的信息结构传入到一个list当中。

4，处理组件将ticket_infofield.py 中的定义的需要提取的信息结构名字的列表（list），和当前xml文件中识别出来的ticket_name部分的信息结构（list）进行比较，如果后者包含前者，这说明文件可以解析，转入后续的环节；否则，则提示当前文件错误。

5，处理组件根据入参中期望解析的csv文件名称和绝对路径，打开一个CSV文件的句柄， 并根据ticket_infofield.py 中的定义的需要提取的信息结构名字的列表（list）顺序，写入CSV的表头数据。

6，处理组件接下来。从当前的xml文件中识别出ticket_data部分的信息结构的取值， 根据并根据ticket_infofield.py 中的定义的需要提取的信息结构名字的列表名字，匹配出需要提取的ticket_data数据，处理完一条ticket交易记录，就在上一步操作中，打开的文件中，写入一行数据；直到把所有的行记录处理完毕。

7，当前的文件处理完毕以后， 处理组件会打开一个结果list，记录本次的处理结果。包含2个信息结构，[process result, xml-file-size],

其中，process result=0，处理成；=1，处理失败。

​     Xml-file-size 记录生成的xml文件的大小。

8， 处理组件，继续完成后续文件的处理，并生成返回值。

9， 全部文件处理完毕以后，处理组件返回，处理结果的返回数值。



### 4.1.3 ticket_infofield.py

​      ticket_infofield.py 是一个python的配置文件，用于配置需要提取的信息结构的字段。

![10-ticketinfo](/assets/python-bigdata-starter/cp04/10-ticketinfo.jpg)

### 4.1.4 返回值

返回值也是一个list， 包含了每个文件的处理结果。

[ [ 0,size],[0,size],[0,xize] ]

![xml 解析结果](/assets/python-bigdata-starter/cp04/xml 解析结果.png)





## 4.2 XML解析思路

​      XML是一种具有自描述特性的数据结构，所以非常适合来表达半结构化数据(semi-structured data)。它是结构化的数据，但是结构变化很大。因为我们要了解数据的细节所以不能将数据简单的组织成一个文件按照非结构化数据处理，由于结构变化很大也不能够简单的建立一个表和他对应。

​      在大数据的应用中，需要和很多外部系统进行数据采集与处理，所以掌握XML数据的解析技术，是大数据处理的一个重要的技能。



### 4.2.1 XML的树形结构

​      要想做好XML的数据解析，首先要理解XML的树形结构。在这里，我们借助XML DOM 来理解相关的树形结构。

XML DOM 把 XML 文档视为一种树结构。这种树结构被称为节点树。

可通过这棵树访问所有节点。可以修改或删除它们的内容，也可以创建新的元素。

这颗节点树展示了节点的集合，以及它们之间的联系。这棵树从根节点开始，然后在树的最低层级向文本节点长出枝条：

   ![1-XML DOM](/assets/python-bigdata-starter/cp04/1-XML DOM.jpeg)



上述图片表达了 XML 文件 [books.xml](http://www.w3school.com.cn/example/xdom/books.xml)。

父、子和同级节点

节点树中的节点彼此之间都有等级关系。

父、子和同级节点用于描述这种关系。父节点拥有子节点，位于相同层级上的子节点称为同级节点（兄弟或姐妹）。

·         在节点树中，顶端的节点成为根节点

·         根节点之外的每个节点都有一个父节点

·         节点可以有任何数量的子节点

·         叶子是没有子节点的节点

·         同级节点是拥有相同父节点的节点

下面的图片展示出节点树的一个部分，以及节点间的关系：

!![2-父子节点](/assets/python-bigdata-starter/cp04/2-父子节点-1550085468755.jpeg)

因为 XML 数据是按照树的形式进行构造的，所以可以在不了解树的确切结构且不了解其中包含的数据类型的情况下，对其进行遍历。

您将在本教程稍后的章节学习更多有关遍历节点树的知识。

**注释：**父节点：Parent Node，子节点：Children Node，同级节点：Sibling Node。



### 4.2.2 待解析XML文件的树形结构

​      借助上面的树形结构的概念，我们来看看银行的ticket交易清单的XML文件的树形结构。

![3-layered tree structurev2](/assets/python-bigdata-starter/cp04/3-layered tree structurev2.png)



​                                           图 4-3 银行 ticket 的XML 树形结构

​       从上图可见， 我们将一个具体的交易清单记录封装在子节点measurement下。  在这个节点下，一共有三个字节点：

子节点1： objecttype 用于描述这个数据结构在现实世界的对象名称。

子节点2：ticket name 用于描述银行交易清单的表头结构（column field） 。

子节点3： ticket data 用于记录多条具体的交易记录的数据。子节点三下面还有多个子节点， 子节点的数量和ticket data中包含的记录数相当。



​         从上面的描述中，聪明的你是否感觉到很奇怪，为什么要把ticket name，和ticket data 分离在不同的子节点中。 因为从现实的事件来看，ticket name 代表的是key, 而ticket data 代表的是 value。           大家直观的想法，是参考fileheader 这个节点的表达方式，将key ，和value 放置在同一个节点中进行表达？



​        这是因为，在现实的情况中，可能会存在着很多条交易记录， 假如将（key，value ）的结构在同一个节点中表达，那么会耗费很多的空间来保存key的信息，而这些信息是完全一致的，存在大量的冗余信息。

​        所以在这里，采用的是将ticket name（key）， ticket data（value） 进行分离保存的方案。 在这个方案中，压缩了key的保存空间，所以比较优势一些。当然，这种处理方案也存在一个缺点，就是在解码的时候，会增加一些障碍。而这正是python的优势，可以使用一些善巧的结构，例如dict 和set来简洁的处理这种情况。

### 4.3.3 XML解析方案简介

​                                                      ![4-ET 解析方案](/assets/python-bigdata-starter/cp04/4-ET 解析方案.png)

​                                   图 4-4  常见的XML解析方案

Python 提供三种场景的XML方案， 其中ElememtTree 的方案 可以提供一种即易于理解，又高效利用内存的解决方案。所以对于立志攻克python大数据的你来说，选择ElememtTree是最具有生产力的一种方案。

在使用Element tree 的时候，常常要考虑使用一种“非阻塞的增量解析法”，相关的文档描述如下：

Most parsing functions provided by this module require the whole document to be read at once before returning any result. It is possible to use an [XMLParser](https://docs.python.org/3.6/library/xml.etree.elementtree.html#xml.etree.ElementTree.XMLParser) and feed data into it incrementally, but it is a push API that calls methods on a callback target, which is too low-level and inconvenient for most needs. Sometimes what the user really wants is to be able to parse XML incrementally, without blocking operations, while enjoying the convenience of fully constructed [Element](https://docs.python.org/3.6/library/xml.etree.elementtree.html#xml.etree.ElementTree.Element) objects.

从上面的文字我们可以发现， 假如我们把整个XML文件加载到内存中，会导致巨大的内存开销； 而假如我们使用一种push API的模式来进行增量解析，需要使用回调的编程模型，这个对非计算机专业的你来说，可能很难理解。所以作为一个大数据的新人，我们最好是选择一种折衷的方案，即能够通过XML文件部分加载的方式进行文件处理，又可以使用一种完全结构化的方案来进行处理。这样兼顾了效率和编程的简洁性。这就是所有的pull API的模式。

具体的参数见下面的文字 说明：

Parses an XML section into an element tree incrementally, and reports what’s going on to the user. source is a filename or file object containing XML data. events is a sequence of events to report back. The supported events are the strings "start", "end", "start-ns" and "end-ns" (the “ns” events are used to get detailed namespace information). If events is omitted, only "end" events are reported. parser is an optional parser instance. If not given, the standard XMLParser parser is used. parser must be a subclass of XMLParser and can only use the default TreeBuilder as a target. Returns an iterator providing (event, elem) pairs.



Note that while iterparse() builds the tree incrementally, it issues blocking reads on source (or the file it names). As such, it’s unsuitable for applications where blocking reads can’t be made. For fully non-blocking parsing, see XMLPullParser.

使用的函数方式是：

```python
xml.etree.ElementTree.iterparse(source, events=None, parser=None)
```



### 4.2.4 XML的后序遍历法

 要想做好XML的文件解析，有一个要义是“从后向前，从内到外”。

   简要解释如下：

1，从后向前： 在识别XML标签的时候，需要定位一个标签，识别出一个标签，会促发一个event，并转入后续的处理，比较好的方式是以定位结尾标签，作为触发事件。具体参数选择是"end"。



 2，从内到外： 在提取数据的时候，先提取最内层（最底层）叶子节点的信息结构，在提取高一层的叶子阶段，采用逐层向上的提取方式，最后提取根节点的信息结构。

​                  ![5,增量加载的方案](/assets/python-bigdata-starter/cp04/5,增量加载的方案.png)

​                                                  图 4-5 增量的提取方案

  从上图可见，整个的处理过程中，是以识别“尾标签”为触发的识别事件。 所以在识别的过程中

1，首先识别出ticket name 中包含的column field的尾部标签 ，总共有8个 取值。也就是8个叶子节点的数据全部识别出来。

2，接下来识别到父亲节点的尾部标签，这就提取了ticket name这个信息结构。这样就完成了针对表头数据部分的识别。

3，接下来识别到第一个ticket data 的 information field 的尾部标签， 提取其中的取值。在这个层级，总共有8个叶子标签。

 4，接下来识别对于的父亲节点的尾部标签，也就是ticket的尾部标签，这标志着第一个行数据的结束。

  随着从上往下的文件数据装载， 循环执行第3步，第4步，直到把所有的行数据都识别成功。

5，最后识别到ticket data 的尾部标签，完成对数据部分的识别。



讨论1： 大家看看，这种从后往前提取的优势。

1，首先采用从尾部标签定位的方式，对信息结构的定界比较简单。可以显式的知道一个信息结构的定界结尾。

2，第二利用从尾部标签定位的方式，可以先定位儿子标签，再定位父亲标签，这样对构造父亲标签的结构比较简单，消除构建数据结构的不确定性。（因为完成父亲标签构造的时候，已经搞清楚了儿子标签的状况）

3, 先识别的底层标签，识别完毕以后，就可以释放叶子节点的数据，这样需要缓存的数据比较少，内存占用比较经济。

   采用这种方案，可以非常优雅的处理XML 文件，而且处理的方式非常的易于理解。



### 4.2.5 面向对象的程序设计

​       虽然python中有大量的面向过程的程序设计，不过在使用XMLPullParser 进行处理的时候，会涉及到多个行数据的结构构造，这个过程在内存中处理比较方便，而这种处理过程，利用对象化的封装可以很好的表达这种复杂的结构，所以在这个章节，要引入面向对象的设计方案。

​     ![6-面向对象的基础](/assets/python-bigdata-starter/cp04/6-面向对象的基础.png)

​                图 4-6 面向对象的设计方案



## 4.3 XML文件解析对象

​      由于xml文件解析是一个非常通用的组件，存在很大的复用价值，所以我们考虑采用对象的方式来封装这个功能，虽然从面向对象理论的角度，要定义面向对象非常的负责，而从应用的对象的角度，我们主要理解如何观察一个对象就ok了。 观察的视角见下图：

​        ![7-对象的内部特性与外部特性](/assets/python-bigdata-starter/cp04/7-对象的内部特性与外部特性.png)

​                                         图 4-7 如何观察一个对象



### 4.3.1 Ticket(object)对象设计

   在本实验中，引入一个对象Ticket（object），用于对XML文件的处理。

![8-xml对象的解析结果](/assets/python-bigdata-starter/cp04/8-xml对象的解析结果.png)

​                                            图 4-8 XML解析对象的设计



1，外部视图： 如果从外部来观察这个对象，我们可以首先理解其中的外部调用接口（operation），相关的传参是待解析的xml文件， 期待生成的xml文件名，以及期望识别的字段结构（以python配置文件的方式）。

2，内部视图： 内部视图包含这个对象的信息结构（information field），以及能够支持的函数接口，也可以称为（behavior）。



所以，从上面可以观察面向对象的设计好处是：

​                                         **易于理解+易于变化**

### 4.3.2 Header+records

​     在实现一个xml文件转换为csv文件的时候，核心是要能够表达相关的信息结构，就是表头和数据。

​     相关的声明信息结构的代码是：



```python
header = OrderedDict() # OrderdDict是顺序存储字典，Dict能够有效映射半结构数据，但Python的默认Dict是随机存储。
records = []
record = OrderedDict()
```

 借助python强大的内生结构，我们可以很方便的表达CSV数据结构。这个结构简要说明如下：

1，records 是一个顺序存储字典，用于记录一个CSV的表头信息结构。

2，records 是一个列表，用于记录CSV文件的多个行数据。一个records是由多个record组成的。

3，record是一个顺序存储字典，用于记录一行数据。

下面是一条记录，供智慧的你来理解这个数据结构。

![header +data dict](/assets/python-bigdata-starter/cp04/header +data dict.png)

​                                               图 4-9 核心的数据结构

### 4.3.3 parse_xml(self)

​        这是此处最重要的一个构造函数，实现对xml文件的解析处理， 相关的解析思路，在4.2.4 XML解析的后序遍历法已经载明。

![5,增量加载的方案](/assets/python-bigdata-starter/cp04/5,增量加载的方案-1550086482819.png)

此处使用了ETree 的核心语句是：

for event, elem in ET.iterparse(self.filename)

\# iterparse 默认使用end event, 即碰到尾标签后生成对应的elem。因此，解析逻辑可以从相关叶子结点写，想当于树遍历算法的【后序遍历】(Postorder Traversal)



### 4.3.4header_check(self)



​         在xml解析的时候，往往不需要提取全部的字段，往往是按需提取字段，所以我们可以确定待解析的XML文件中，是否包含了期待的数据结构。所以我们就设计了一个函数header_check(self)，用于判断待解析的文件，是否可以正常进行解析。

```python
header_def = set(self.header_def) #将Dict转换为Set，便于进行比较操作
header = set(self.header.values()) #Dict的keys()和values()返回iter key or value
return header <= header_def #Python Set 可以使用 <= 表达 issubset操作。
```



### 4.3.5 输入数据后缀识别

​       此处，还有一个重要处理，就是针对输入文件，识别是否是xml的格式，这样有一个好处，就是如果有其他格式的文件，还可以添加其他的处理逻辑。

​       比如，我们可以考虑一个对象，同时支持json和xml文件的解析。



## 4.4 设计模型

   ![9 object model](/assets/python-bigdata-starter/cp04/9 object model.png)



​                                   4-10 设计模型

​       从上面的设计模型可以发现，利用强大的面向对象设计，整个方案变得很简洁。利用一个控制器对象，对输入的XML文件对象列表，进行顺序解析，具体的解析是调用前期设计的ticket（object）对象，就可以得到期望的csv文件。



### 4.5 总结

以上是 XML的原型实验内容。 从这个原型实验中，我们可以发现：

1，使用python的Element Tree组件，可以很方便的实现一种基于“后序遍历法”的解析策略。

2，使用面向对象的处理思路，可以很方便的实现一些复杂的内存操作。而面向对象的核心特点是易于理解和易于变化。而得益于python强大的抽象能力，可以表达一些和客观世界非常接近的对象。

3，内存的精益管理，是大数据处理的一个重要基础。
