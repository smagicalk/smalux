import type {Config} from '@docusaurus/types';
import type {Options, ThemeConfig} from '@docusaurus/preset-classic';
import {themes as prismThemes} from 'prism-react-renderer';

const config: Config = {
  title: 'Smalux 文档',
  tagline: '轻量、可靠、可扩展的监控探针',
  favicon: 'smalux.png',

  url: 'https://smagicalk.github.io',
  baseUrl: '/smalux/',
  organizationName: 'smagicalk',
  projectName: 'smalux',
  trailingSlash: false,
  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  // 直接复用仓库已有品牌资源，避免文档站维护第二份图片。
  staticDirectories: ['static', '../assets'],

  i18n: {
    defaultLocale: 'zh-CN',
    locales: ['zh-CN'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          showLastUpdateTime: true,
          showLastUpdateAuthor: true,
          editUrl: 'https://github.com/smagicalk/smalux/edit/master/website/',
        },
        blog: false,
        pages: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Options,
    ],
  ],

  themeConfig: {
    image: 'smalux.png',
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Smalux',
      logo: {
        alt: 'Smalux',
        src: 'smalux.png',
      },
      items: [
        {type: 'docSidebar', sidebarId: 'docsSidebar', label: '文档', position: 'left'},
        {
          href: 'https://github.com/smagicalk/smalux',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: '开始使用',
          items: [
            {label: '项目概览', to: '/getting-started/overview'},
            {label: '快速开始', to: '/getting-started/quick-start'},
            {label: '源码构建', to: '/installation/source-build'},
          ],
        },
        {
          title: '开发与协议',
          items: [
            {label: 'Workspace', to: '/development/workspace'},
            {label: 'Protocol', to: '/protocol/overview'},
            {label: '扩展 Task', to: '/extensions/task'},
          ],
        },
        {
          title: '项目',
          items: [
            {label: 'GitHub', href: 'https://github.com/smagicalk/smalux'},
            {label: '问题反馈', href: 'https://github.com/smagicalk/smalux/issues'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Smalux contributors.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['rust', 'protobuf', 'toml', 'powershell', 'bash'],
    },
  } satisfies ThemeConfig,
};

export default config;
