---
title: "Python BigData MicroService CP01"
cover: "2.jpg"
category: "python"
date: "2019-07-20"
slug: "python-bigdata-MicroService-cp01"
tags:
---

# 大数据微服务 第一章 如何进入大数据的处理的世界

## 1         背景介绍

1.1     大数据处理的困惑

随着5G，物联网，大数据的流行，大数据俨然已经成为计算机科学这个王冠上的钻石，很多朋友都希望进入这个领域，一展才华，从中赚到人生的第一桶金。 但是如何利用最短的时间进入到大数据的世界，确是是大家一直非常困惑的问题。

虽然大数据是计算机科学的一个分支，但是目前在大学中并没有系统的培训教材来训练这方面的技能。假如你是一个大学生，该从哪里开始入手呢？

在2006年，我们需要涉足大数据领域的时候，这个问题也非常的困扰， 好在当时遇到澳大利亚的专家，他的建议是：

1. 尽量不要使用hadoop+Hive的方案，因为效率比较低。

2. 尽量不要使用spark+scala 的方案，因为难以驾驭。

3. 充分使用python+关系型数据库的方案，因为集成效率比较高，注意尽量少的使用内存，尽量多的使用CPU。

因为当时，我们没有什么经验，所以就按照这位专家的建议，选择了python+postgres-sql+green-plum 的方案，作为我们的核心方案。





## 1.2     大数据的4“V”难题



![大数据的4V 难题](/assets/python-bigdata-microservice/cp01/大数据的4V 难题.jpg)

图 1  大数据的4V难题

随着5G，物联网和工业4.0的普及，大数据人才需求量日益大增， 但是大数据处理和我们传统的数据处理存在很大的不同。 而大数据处理的第一个难题就是velocity 难题。也就是说，我们需要非常高效的处理数据。

我们知道任何一个大数据项目的预算是有限的，这其中有一个瓶颈就是计算资源方面的预算，无论是租用公有云平台，还是自己搭建私有云平台吗，都是一笔昂贵的开销，所以如何使用最少的硬件来满足velocity 难题的挑战，是大数据项目成败的关键。

而要破解这个velocity难题，有一个非常重要的要求就是：

**1，尽量高的利用CPU资源。**

**2，尽量低的利用内存资源。**

**3，在多机的环境下，一定要把每台服务器都充分的使用。**

  要实现这个技术要求， 我们需要以下方面的专业组件

   1，灵活的任务调度框架

   2， 基于生产者-消费者模型的任务拆分

   3， 服务的异步化协调。

上述的原则虽然讲解起来比较通俗易懂，而实现起来确实比较抽象和艰困的，因为这些设计原则以及设计到大数据方案中最本质的技术难点，我们还是结合原子用例的方式来进行介绍。



## 1.3      Git的起点

选定了方案以后， 当时一位好朋友建议我们使用celery作为分布式计算的框架，因为这位好朋友正在研究Scrapy 技术。

![figure2 Queue component](/assets/python-bigdata-microservice/cp01/figure2 Queue component.png)

图2 分布式Queue组件

而对于我们当时的技术水平来说，要应用这样的框架，真是力不从心。因为前期我们用python写的ETL代码，基本上是按照紧密耦合的方式进行处理的，而是要celery就意味着需要把这些ETL代码分离成为异步的计算组件。如何下手就非常的困难， 所以我们就在GIT上进行寻找，最好幸运的发现了一个项目。

根据这个项目留下的邮件，我们也非常幸运的联系到了作者，由于他的项目中留下了一个ETL组件拆分的范例，所以经过原型实验，成功的理解了基于异步微服务方式来进行ETL组件的设计方法。经过了2个月的实验，我们最好成功的将现有的代码进行了重构，形成了基于异步微服务方案的ETL框架。

## 1.4     持续精进

在完成了第一个ETL 组件的改造方案以后，我们还有3套基于传统方案的ETL采集项目。 经过了大半年的工作，整合工作，成功的实现了四套采集程序，整合在一个集群中运行。由于需要采集的数据源比较多，为了更好的消除冗余，方便升级。 我们又进一步将四套采集程序进行融合，提炼了抽象父类，初步实现了采集框架的融合。

在这个过程中，我们也比较深入的学习了celery 的一些高级特性，可以实现弹性更大的配置方法。在我们最近的一个项目中，实现了每小时实时处理60万个文件，利用了150个容器，输入文件的总大小大约为0.5G的压缩格式文件，压缩比为5-10倍。

经过这个项目的历练，已经基本胜任了常规的大数据项目的ETL处理工作。



## 1.5     回到本源

当数据的规模大到无法处理的时候，我们必须要能够通过多台物理机来对数据进行处理， 而当ETL组件需要部署在多台服务器上的时候，如何合理的组织多个ETL组件，就成为一个难点。

所以，希望管理相关的复杂度， 就需要能够将一个紧密耦合的ETL组件，合理的拆分为多个独立的微服务，这些微服务是通过异步的方式进行处理协同操作。

  所以，无论多么复杂的ETL应用，只有能够合理的拆分为微服务，就可以方便的部署到多台服务器上，通过集群的方式来处理。  

 所以，微服务就是大数据处理的核心本源，而如何将一个ETL应用，拆分为合理的微服务则是一个大数据项目的关键。一方面，通过微服务的组合可以完成用例的要求；另外一方面，通过微服务的并行化运算，可以非常方便的实现数据的快速处理。这两方面的问题处理好了，就兼顾了正确性和效率的要求，那么大数据项目就比如会成功的。



## 1.6     从“入门”到“登堂入室”

在第一门课程的学习中，我们掌握了pthon在数据处理的基础技能，可以胜任常规数据的处理，在这一门课程中，我们将深入的学习如何进行微服务拆分的处理，这样就可以胜任大数据的任务处理。

我们将要学习的框架见下图：

![figure1 ETL component](/assets/python-bigdata-microservice/cp01/figure1 ETL component.png)

在这本书的学习中，我们将会分析几个真实的案例，并透过这个云原生的微服务方案来实现我们的应用。

从第二门课开始，我们的主要的实验都是base 在linux服务器上，请大家做好准备吧。
