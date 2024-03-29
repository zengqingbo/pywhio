---
title: "Python BigData Starter CP03"
cover: "3.jpg"
category: "python"
date: "2019-02-12"
slug: "python-bigdata-starter-cp03"
tags:
---



多文件合并是python大数据处理的一个基础，下面我们来看看如何通过简洁的代码来实现相关的应用。

# 3 多文件合并

## 3.1 问题背景介绍

​       在这个应用中，我们需要将多个交易清单合并成为一个聚合的交易详情文件，利用这个交易详情文件，可以方便的实现后续的数据入库处理，或者是内存装载处理。

​       下面将相关的需求的IPO 分析如下



### 3.1.1 输入数据分析（I）

​     为了简化问题的分析，我们假设在上1个15分钟周期粒度产生了3张ticket清单。**分别是**

QLR2019020710-001.csv

QLR2019020710-002.csv

QLR2019020710-003.csv

三个输入文件的路径均在**D:****盘的根目录**下。      

在这里，我们假设交易发生在银行是财富银行的QLR （青年路）网点。



### 3.1.2 处理过程（P）

​     下面来看看用例“**多文件合并”**对应的处理过程**：**

1、用户界面向处理组件传递参数，入参包括 需要合并的文件清单，以及期望的合并文件名称。

2，处理组件接收入参以后， 首先根据文件清单中的文件绝对路径，通过文件系统，识别需要处理的文件列表。

3，处理组件提取第一个文件，根据文件的后缀名称确定为CSV格式，就调用CSV处理组件，将文件流转换为中间流。并将其中的信息结构拷贝到一个临时中间流，**临时中间流**中包含第一个文件的所有数据，含表头数据（key），以及所有行数据包含的信息结构数据（value）。

4，当第一个文件处理完毕， 处理组件将关闭第一个文件，顺序提取第二个文件，将文件转换为中间流。 针对第二个文件，处理组件将先识别出表头数据（key），再识别出其中所有行数据的信息结构（value），并将所有行数据的信息结构（value）追加拷贝到**临时中间流中**。

5、处理组件将顺序处理后续的文件，直到所有的文件中包含的信息结构都拷贝到**临时中间流中**。

6，处理组件将**临时中间流**转换为一个**文件流**，文件流的绝对路径 将将根据接收入参中“期望的合并文件名称”来生成。

7，文件生成完毕以后，处理组件将提取合并文件的尺寸信息。并返回结果。 返回数值中包含两个信息结构，首先是处理是否成功，第二是如果成功，合并文件的尺寸信息。



### 3.1.3 输出数据（O）

   由于这个用例比较简单，所以我们就不阐述相关的处理过程，处理的目标是将三个文件合并为一个文件。合并的文件命名为：

QLR2019020710merge.csv

   输出文件的路径在D: 盘的根目录下。



## 3.2 分析模型（analysis model）

​       分析模型是面向对象的软件工程（OOSE）方法中一个非常重要的设计环节，使用分析模型的目的是为了能够在开始详细的程序设计以前，建造一个合理的对象模型，来保障对用例模型的支撑。在这个阶段，不需要考虑编程语言和外部中间件，仅仅需要考虑使用正确的逻辑来支撑用例的执行。在确认对象模型能够正确的支持用例模型以后，会转入后续的设计环节。

​        根据这个用例，我们考虑的分析模型见下图所显示：

  ![分析模型之UC1 CSV merge](/python-bigdata-starter/cp03/分析模型之UC1 CSV merge.jpg)

​                                                      图 3-1 分析模型

​        从上面的分析模型可见，虽然是一个简单的用例，其中包含了最基本的stream 处理的核心过程，这个也是后续大数据处理的基础。

## 3.3 接口归一化

​       考虑到后续模型的复用和组合，下面将相关的接口进行明确。

### 3.3.1 CSV文件格式

​       CSV格式有很多种，主要的差异在分隔符的选择上，在本案例中，我们选择常规的逗号分隔方式，
这个也是使用office 套件将excel文件转换为CSV文件的标准导出格式。



### 3.3.2 输入参数

  在本用例中，设定两个入参

1， 需要合并的文件list

 [' D:\QLR2019020710-001.csv ', ' D:\QQLR2019020710-001.csv', ' D:\QQLR2019020710-001.csv']

2，期望压缩的文件名称

   D: \QLR2019020710merge.csv 



### 3.3.3 返回值

1、返回压缩的结果，返回值 0（正常），1（报错）

2、打印输入文件及大小， 输出文件及大小

 

备注：设计约束

演示CSV模块，CSV逗号分隔（EXCEL导出格式）

3、输入参数，list  ，  结果（含文件名）

4、返回值 0（正常），1（报错）

5、打印输入文件及大小， 输出文件及大小



## 3.4 设计模型

​       ![3-2 object model](/python-bigdata-starter/cp03/3-2 object model.png)

​              图 3-2    文件合并的对象模型

 

从上图可见， 以上是整个CSV文件合并的对象模型。 在这个对象模型中，核心的对象是**merge（filelist，dist）** 他负责实现文件的额合并功能。

   在整个的代码部分，包含了三个函数，分别是：

1，get_FileSize(filename) ，用于读取文件的大小。

 

2，csv_reader(filename)，用于读取一个CSV文件，将文件流转换为一个中间流，中间流的形态为python的list对象，是二维数组的形式。

 

3，merge(filelist, dist)， 用于将文件进行合并处理， 入参是需要合并的文件列表，以及需要生成的文件名称（包含决对路径）。

### 3.4.1 csv_reader(filename)

​     此函数用于实现相关的CSV文件的转换功能，由于本实验主要是用于理解流转换（stream IO）的处理过程，所以我们假设是一个常规的CSV文件，不考虑超大文件的情况。

![3-1 文件流转换](/python-bigdata-starter/cp03/3-1 文件流转换.jpg)

​       另外，考虑到中文处理的问题，在读取csv文件的时候，增加了参数：

​       encoding="gbk"



### 3.4.2 merge(filelist, dist)

​      此函数，用于实现文件的合并的功能。整个组件的实现和前面的IPO处理过程是吻合的。要点简介如下

1，生成临时文件流。

其中核心的CSV文件输出的代码是：

with open(dist, 'w', newline='', encoding="gbk") as distfile

此处distfile 是一个临时文件流，用于暂存多个文件合并的信息结构。

 

2，生成CSV的表头行数据

```python
if header == [] and len(csv_rows)>0:
	header = csv_rows[0]
	csvwriter.writerow(header)
```

   

通常是在处理第一个文件的时候，会生成表头数据， 这里有一个逻辑判断，就是 如果临时文件流的第一行非空（尚未写入distfile的第一行），且第一个输入文件转换的中间流的行数超过1行，将中间流的第一行作为表头数据，写入到临时文件流中。

 

3，文件合并检查

由于文件合并的目的，是为了方便后续的数据入库，所以要保障所有的文件都是具有相同的列信息结构，如果存在不一致的情况，则会返回合并失败的提示。

​     if csv_rows[0] != header :  return 1



## 3.5    总结

​     以上是 文件合并操作的原型实验内容。 从这个原型实验中，我们可以发现：

1，python原生提供的CSV组件，可以提供非常强大的流转换的操作能力，方便我们对文件进行各种处理操作。

2，理解流转换的操作过程，可以方便我们对数据进行各种弹性的处理。

3，将组件封装为接口的形式，可以很方便的在后续的项目中进行复用。

 