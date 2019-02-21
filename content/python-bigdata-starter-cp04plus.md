---
title: "Python BigData Starter CP04 plus"
cover: "8.jpg"
category: "python"
date: "2019-02-16"
slug: "python-bigdata-starter-cp04plus"
tags:
---
​
         XML转换为csv的原型试验，虽然看起来很简单，其实蕴含了很多python大数据处理的设计思想，python中一个重要的数据结构list，以及面向对象程序设计的重要原则，所以，我们再次回顾一下这个原型试验，

## 4.6 从面向对象看XML解析



​         由于XML是一种非常重要的数据结构，所以在这里我们针对相关的知识点在做一些补充说明。

### 4.6.1 XML树形结构



![4-11XML 树形结构20190215](/assets/python-bigdata-starter/cp04/4-11XML 树形结构20190215.png)

   图 4-11 关键的对象信息结构。

​       从上图可见，在一个XML文件中， 用于表达一个CSV文件的和兴部分。 由于为了节省存储空间，所以将CSV的表头和CSV的行数据进行了分开保存。 具体来说

1，CSV的表头部分，保存在ticket name中

2，CSV的多行数据，保存在ticket data 中，不同的行数据，通过id来进行区分。

3，CSV的一行数据，保存在ticket中。在一行之中，不同列是通过i的编号来区分，由于ticket name 中也有相同的i编号机制，所以通过i编号，就可以实现 {（指标名,指标值）} 之间的关联。



### 4.6.2 如何构造对象来实现解析

![4-12 refined-ticket object model](/assets/python-bigdata-starter/cp04/4-12 refined-ticket object model.png)

​       图 4-12 object model refine

  从上图可见，相关的对象模型是支持XML文件的解析出来。  

​       那么我们来看看，构造这个对象的主要过程，设计三个方面。

1， 操作（operation）：假如我们从外部来观察这个对象，最重要的是这个对象对外部提供的操作，这里是初始化的方法， 在初始化的时候，需要self （对象本体），filename（外部传递的CSV文件名），header_def( 期望生成CSV的表头数据)。

2，信息结构（attribute）： 对象设计的一个要点是信息隐藏（information hiding），也就是说，数据在对象内部来说吗，不同的函数之间是可以方便共享的，而从对象外部来说，这些信息结构是不可见的。

那么其中最重要的三个信息结构，就是：

2.1 header ：是一个顺序字典（ordered DICT），用来表达从xml文件中获得顺序表头数据，是一个顺序字典，其中的key 是序号，value是表头的名称。数据来自XML文件中的ticket name 标签，

2.2 records ：是一个列表，用于表达多条行记录。

2.3：record ：是一个顺序字典（ordered DICT），用来表达从xml文件中获得顺序表头数据，是一个顺序字典，其中的key 是序号，value是指标的取值。数据来自XML文件中的ticket name 标签，

2.4 head _def 期望生产的CSV文件的表头字段。是一个列表（list），默认情况下是有序的。

 3 ，函数功能（behavior）： 主要包含了三个功能

###  4.6.3信息隐藏（information hiding）

​       在这个对象的设计中，有一个重要的概念是信息隐藏，因为在面向对象设计的一个重要理念是:“尽可能的将信息结构的修改限制在局部”，这样在对对象结构做调整的时候，影响的范围会比较小。要做到这一点， 对象内部的数据结构，一般来说，是不允许对象外部的其他对象来访问的。 这样修改一个对象，就不会影响其他的对象。

​         在我们这个案例中，核心的问题是要实现信息结构从xmL文件到CSV文件的转换，这个过程是在内存中转换。 要解决这个问题，需要有两个要素，1个要素是领域对象中包含的信息结构，而第2个要素是针对信息结构的处理算法，比如转换为CSV格式，转换为json格式，或者是其他希望的格式。

 相关处理的对象模型见下图：

 ![4-13-面向对象设计信息隐藏](/assets/python-bigdata-starter/cp04/4-13-面向对象设计信息隐藏.png)

​      图 4-13 利用OO方法实现数据对象的封装

​        从上图可见，无论是数据处理，还是大数据处理，都要面对数据框（data frame）的处理问题，无论是CSV文件，关系型数据库，XML文件，内存中字典（dict），本质上都需要处理这种数据组织结构。由于现实世界的数据格式多种多样，所以处理的函数（方法）会有很多种，那么如何将数据方便的传递给这些函数，或者说如何让不同的函数（方法）能够方便的访问到这种数据组织结构？

​         在面向过程的编程中，要方便的实现这种功能是非常艰困的，而在面向对象的编程中，由于对象可以实现对数据和方法的联合封装，所以只要在数据不改变的前提下，添加方法是非常简单的一件事情。很多人听到面向对象的编程，都会感觉很“不屑一顾”，因为一个文件解析，还有用到对象封装，简直是“高射炮打蚊子”，这种想法固然是有一定道理。但是在大数据的开发中，因为数据的源头很多，数据格式也非常多，所以数据的预处理的编程工作量会非常的大，要压缩开发成本，面向对象的开发方式就显得至关重要。 在这个时候，技术已经没有高下之分，唯一的区别是成本的大小之分，最好胜出的都是低成本的解决方案。



## 4.7     列表生成式（即List Comprehensions）

​        本课件面向的对象是具备一定的python基础的工程师，并立志于进军大数据处理领域，将信息领域的数据分析知识和专业领域的应用问题进行融合，形成跨界的应用价值。 所以本教程中并不会侧重介绍python的语法。

然而，列表生成式实在是太重要，他可以极大的简化代码的编写，所以是必须掌握的一种语法知识。





### 4.7.1   列表生成式简介

​       运用列表生成式，可以快速生成list，可以通过一个list推导出另一个list，而代码却十分简洁。

关键范例如下 ：

1、列表生成函数：

```
>>> range(1, 11)
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
```

2、利用列表生成列表

```
>>> [x * x for x in range(1, 11)]
[1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
```

3、条件过滤

```
>>> [x * x for x in range(1, 11) if x % 2 == 0]
[4, 16, 36, 64, 100]
```

4、循环生成

\>>> [m + n **for** m **in** 'ABC' **for** n **in** 'XYZ']

['AX', 'AY', 'AZ', 'BX', 'BY', 'BZ', 'CX', 'CY', 'CZ']

5、条件生成

```
>>> [x if x>5 else 0-x for x in range(1, 11)]
[1, 2, 3, 4, 5, -6, -7, -8, -9, -10]
```



### 4.7.2 面向“列表”的编程语言

​        Python最近今年比较火，大家可能会认为python这种程序设计语言是一种非常年轻的编程语言，他其实诞生于1989年，也算是一门非常古老的脚本解释语言。 但是在早期python并不流行，其中一个很重要的原因，早期的python编程的时候，代码编写格式比较麻烦，也很不方便阅读。

​          好在python的开发团队是一个非常坚持的团队，持续对这门语言进行升级，其中一个很重要的演进就是对list功能的持续完善，并引入了列表生成式的功能，在善于利用列表生成式的情况下，代码可以非常的简介。

​           所以，Python可以算是一种**“面向列表”**的编程语言。他的设计思路是，当我们提炼了领域对象模型以后，在实现领域对象模型的时候，尽量使用list的方式来进行表达，而利用list原生的方法，可以非常简化的实现一些复杂的处理过程。 所以智慧的你，抓紧时间学好list，就要很多时间去逛街，运动，看电影，并赚到大把的银子。



## 4.8   如何利用列表生成式来处理CSV输出

### 4.8.1 用例故事回顾

​        本用例中，需要从XML文件中按需提取信息结构，转换为一个CSV格式的文件。 也就是说，并不需要将XML中全量的信息结构，转换为CSV格式的文件。所以，我们需要使用过滤器（filter）的功能，过滤出需要的信息字段结构，来生成最终的信息结构。



### 4.8.2  生成CSV代码简介

![4-14header +data dict](/assets/python-bigdata-starter/cp04/4-14header +data dict.png)

![4-14-CSV 生成代码](/assets/python-bigdata-starter/cp04/4-14-CSV 生成代码.jpg)

​        图 4-14  基于列表生成式的处理过程

​    从上面可见，生成的过程是

1，首先是通过传参，确定Csv文件的名字，并添加一个文件后缀.csv.

2, 根据文件名，打开一个文件流，生成相应的句柄。

3，创建一个生成csv文件的实例csvwriter，他具备各种csv写操作的方法，传入的参数是上一个环节创建的文件句柄。

4，访问对象Ticket（object）的信息结构self.header_def , 他是一个list，保存了期望生成csv文件的表头数据，将相关的信息结构按照csv的格式写入到csv文件的表头数据中。

5，访问对象Ticket（object）的信息结构self.records, 他同样是一个list，这个list的每一个元素代表从原始XML获取的一个行数据，list中的每一个元素是通过有序字典（Ordered dict）的形式来进行表达。根据self.records的列表长度，就可以知道需要对多少行数据进行转换写操作。

6，从self.records提取一个元素，这个元素是一个有序字字典（Ordered dict），字段的key 是column name， value 是 column data；遍历有序字字典的每一个key， 如果发现一个key和列表self.header.def中的元素匹配，就写入到一个叫做row的list中；选定record全部的元素遍历完毕以后，就根据self.header.def的信息结构顺序写入一行数据，保存在一个叫做row的list当中。没有匹配的key则不会写入到文件系统中。

6，将row写入csv文件的一行中。

7，根据上面的方法，将self.records中所有的元素都写入到csv文件中。CSV文件生成完毕。

​    经过上面的代码，不但实现了CSV文件的生成，也实现了根据过滤器self.header_def的顺序来提取希望的信息结构。 这个操作虽然从逻辑上非常简单，不过真的要使用程序来实现，必然免不了写一大堆循环操作，现在有了列表生成式这个神器，智慧的你就可以很优雅的完成这一段代码。