import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: '开始使用',
      collapsed: false,
      items: ['getting-started/overview', 'getting-started/quick-start'],
    },
    {
      type: 'category',
      label: '安装',
      items: ['installation/source-build', 'installation/agent', 'installation/server'],
    },
    {
      type: 'category',
      label: '使用',
      items: [
        'usage/job-task',
        'usage/collectors',
        'usage/probes',
        'usage/process-socket',
      ],
    },
    {
      type: 'category',
      label: 'Protocol',
      items: ['protocol/overview', 'protocol/session', 'protocol/security'],
    },
    {
      type: 'category',
      label: '开发',
      items: ['development/workspace', 'development/quality', 'development/protobuf'],
    },
    {
      type: 'category',
      label: '扩展',
      items: ['extensions/task', 'extensions/plus'],
    },
    {
      type: 'category',
      label: '部署',
      items: ['deployment/topology', 'deployment/reverse-proxy', 'deployment/github-pages'],
    },
    {
      type: 'category',
      label: '参考',
      items: ['reference/commands', 'reference/project-status'],
    },
  ],
};

export default sidebars;
