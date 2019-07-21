---
title: "Python BigData X CP01"
cover: "1.jpg"
category: "python"
date: "2019-06-05"
slug: "python-bigdata-x-cp01"
tags:
---

# 1    背景介绍

​     目前业界有很多款划时代的数据仓库产品，他们可以基于通用的X-86服务器实现超大规模的数据处理，可以非常敏捷的用于构建一个组织的数据资产，为企业创造数据信息化的价值。这些数据仓库的确是当代的信息化建设的一款神器，可以基于比较低的成本，为企业产生巨大的经济效益。

​     虽然业界也提供了大量是开源产品，多家咨询公司也提供了完备而详细的文档和技术支持管道，然而目前在国内使用开源数据仓库方案依然不多，而且项目的实施往往比较艰困，无法按时交付。究其原因，是因为很多项目在ETL开发上效率比较低，导致项目迭代和交付的进度比较慢，无法释放开源数据仓库组件的巨大能量。

​     俗话说“**工欲善其事，必先利其器**”，要想用好数据仓库，首先要有能力打造高性能的ETL组件，所以如何构建一种100%云原生的ETL方案，是取得数字化转型的一个关键。《易经》有云“天一生水，地六成之”，所以ETL组件在一个数据仓库项目中的作用至关重要，所以掌握一个可靠的数据仓库是很重要的。

# 2   总体方案介绍

![figure1 ETL component](/assets/python-bigdata-x/cp01/figure1 ETL component.png)

   基于100%云原生服务的ETL平台采用的是一种低成本的解决方案，相关的整体解决方案有5部分组成，相关的总体架构简要说明如下：

1、**硬件与OS层**：底层的硬件是基于通用的X86物理机 和centOS 7 的linux 操作系统。

2、**代码仓库层**：运行在centOS 7上的软件主要是放在在代码仓库中，其中主要包含了编程语言，第三方中间件，应用程序方面的软件代码包。

3、**镜像仓库层**：软件代码会转换成为一个镜像，可以用于分发到容器中进行运行。

所有待运行的软件镜像会保留在镜像仓库中。

4、**容器管理层**：项目中需要的多个容器需要在一个容器集群中协同运行，所以需要一个容器编排管理服务，它负责管理和协同多个容器之间的协同工作。

5、**应用层**：在容器集群中，运行了多个应用容器，负责实现对应用的功能。而在应用层，还有两个字层。

5.1 **分布式消息管理APP**：负责提供分布式的消息通信，是基于可靠的高级消息队列协议（AMQP）的分布式消息通信管理。目前Rabbit-MQ 业界最优化一种解决方案。

5.2 **应用层**：负责面向企业级应用，提供数据ETL，数据汇总，地图管理，空间匹配方面的服务。

以上是整个的ETL的总体架构，下面将其中的几个关键部分进行阐述。



## 2.1     代码仓库层

​     我们选择GIT—Lab作为代码管理仓库，和应用项目的代码保存在其中， 整个5G集中网管平台的应用软件包划分为三部分，详细见下图。

![figure2 GITlab](/assets/python-bigdata-x/cp01/figure2 GITlab.png)

代码规划划分：

opdata: 数据ETL流程，集成getter,handler,loader

pgtask: postgresql相关任务，涵盖更改分区，慢查询统计，数据汇总，数据核查等功能。

monitor：相关监控任务，包括从promethus获取监控信息，生成监控报表等相关代码。

   清晰的代码包规划是云原生服务的起点，是非常重要的一个规划。





# 2.2   分布方式消息服务层

 ![figure3 rabbitmq](/assets/python-bigdata-x/cp01/figure3 rabbitmq.png)

  对于ETL来说，处理效率和可扩展性是非常重要的特性，要支持这种特性，就必须采用微服务架构，将应用分解为生产者和消费者，而生产者和消费者之间需要有一个强大的分布方式队列服务。

  而Rabbit-MQ就是这样一个非常强大的分布方式消息服务中间件，可以实现大规模的集群服务支持。



# 3    发现数据的价值

构建一个IT系统，其目标就是要发现和利用数据的价值，那么数据的价值层次见下图：

![figure4 数据处理层次](/assets/python-bigdata-x/cp01/figure4 数据处理层次.png)

​      图3-1 数据处理的层次



从上图可见，建造一个IT系统，基本上需要处理四个层面的问题，就是：

1，首先数据能够存储。

2，其次是利用工具可以查看到数据

3，第三是能够对数据进行各种各样的加工，产生信息。

4，第四是能够在信息中加以提炼，找出正确的内容，形成情报。

一般来说，越上层的结果，对客户的价值越大，越底层的内容，对客户的价值不大。



​      开源的数据仓库对以上数据应用的四个层次都提供了良好的支撑，所以是当下非常强大的一款数据服务利器，而要驾驭这样神器的数据利器，你需要的就是100%云原生的ETL组件。

虽然100%云原生的ETL组件的体系结构看起来非常的简单，但是要完全的掌握这种结构，需要三个前置条件：

首先是面向对象的软件工程设计能力；第二是要有敏捷开发的技术能力，第三要有开阔的国际化视野。

​     一个开发团队如果想依赖自力更生来掌握上面三种能力，往往需要非常漫长的路要走，而假如如果能够找到一个好的合作伙伴，就可以极大的缩短这个过程，获得数字化的红利。所有立志于在大数据领域挖掘金子的小伙伴，都要尽快的掌握100%云原生工具的设计能力，这样就能够乘着大数据的东风，获得数据的价值。