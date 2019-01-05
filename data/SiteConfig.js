module.exports = {
  siteTitle: "Python园地", // Site title.
  siteTitleShort: "pywhio", // Short site title for homescreen (PWA). Preferably should be under 12 characters to prevent truncation.
  siteTitleAlt: "Python交流", // Alternative site title for SEO.
  siteLogo: "/logos/logo-1024.png", // Logo used for SEO and manifest.
  siteUrl: "https://www.pywhio.com", // Domain of your website without pathPrefix.
  pathPrefix: "/", // Prefixes all links. For cases when deployed to example.github.io/gatsby-material-starter/.
  fixedFooter: false, // Whether the footer component is fixed, i.e. always visible
  siteDescription: "Python交流", // Website description used for RSS feeds/meta description tag.
  siteRss: "/rss.xml", // Path to the RSS file.
  siteFBAppID: "", // FB Application ID for using app insights
  siteGATrackingID: "UA-47311644-4", // Tracking code ID for google analytics.
  disqusShortname: "", // Disqus shortname.
  postDefaultCategoryID: "Tech", // Default category for posts.
  dateFromFormat: "YYYY-MM-DD", // Date format used in the frontmatter.
  dateFormat: "YYYY-MM-DD", // Date format for display.
  userName: "Pywhio Group", // Username to display in the author segment.
  userTwitter: "", // Optionally renders "Follow Me" in the UserInfo segment.
  userLocation: "武汉", // User location to display in the author segment.
  userAvatar: "/logos/python-logo-large.png", // User avatar to display in the author segment.
  userDescriptionSub:"《 Python工程师》是一个致力于推广利用python进行分布式并行计算，大数据敏捷项目开发的论坛。",
  userDescription:"我们分享的案例是利用多CPU异步协同计算模式，使用最少的开发成本实施商业级的大数据应用；实现低成本大数据淘金的目的。对于有基本python开发基础的工程师，借助我们的案例，经过大约30个工作日的原型试验就可以胜任大型的商业大数据项目集成工作。 分享的方案全部植根于在用的大数据项目，和国际上的主流方案接轨，可以协助工程师打造国际化视野，提升敏捷开发素养。我们的愿景是：“项目交的出去，客户进得来，工程师发大财”，如果我们的读者希望早日实现财务自由，成为人生的赢家，欢迎关注我们的项目，和我们一起赢。", // User description to display in the author segment.
  // Links to social profiles/projects you want to display in the author segment/navigation bar.
  userLinks: [
    {
      label: "GitHub",
      url: "https://github.com/Vagr9K/gatsby-material-starter",
      iconClassName: "fa fa-github"
    },
    {
      label: "Twitter",
      url: "https://twitter.com/Vagr9K",
      iconClassName: "fa fa-twitter"
    },
    {
      label: "Email",
      url: "mailto:pywhio@qq.com",
      iconClassName: "fa fa-envelope"
    }
  ],
  copyright: "Copyright © 2019. 鄂ICP备18029509号-1" // Copyright string for the footer of the website and RSS feed.
};
