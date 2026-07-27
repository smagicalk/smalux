---
title: 文档站部署
description: 本地构建并通过手动 GitHub Actions 发布 Docusaurus 文档。
---

# 文档站部署

文档站位于 `website/`，使用 Docusaurus、React、TypeScript 和 pnpm。现有源码 README 保留原位置，网站
页面在 `website/docs/` 独立维护。

## 本地预览

```powershell
Set-Location website
pnpm install
pnpm start
```

开发服务器默认只用于本机预览。修改配置或依赖后如果缓存异常：

```powershell
pnpm clear
pnpm start
```

## 生产构建

```powershell
pnpm typecheck
pnpm build
pnpm serve
```

生成文件位于 `website/build/`，已加入 `.gitignore`，不提交到源码分支。

## Pages 地址

当前配置面向项目 Pages：

```text
https://smagicalk.github.io/smalux/
```

因此 `docusaurus.config.ts` 使用：

```text
url     = https://smagicalk.github.io
baseUrl = /smalux/
```

使用自定义域名时，应在 GitHub Pages 设置域名，并把 `baseUrl` 调整为 `/`；不要只修改 DNS。

## 手动发布

仓库 workflow 当前只启用 `workflow_dispatch`：

1. 打开 GitHub 仓库的 **Actions**；
2. 选择 **Deploy documentation to GitHub Pages**；
3. 点击 **Run workflow**；
4. 构建成功后，由 `github-pages` environment 发布 artifact。

第一次使用前，在仓库 **Settings → Pages → Build and deployment** 中把 Source 设置为 **GitHub Actions**。

## 启用自动发布

workflow 中保留了被注释的 `push: branches: [master]`。需要自动发布时取消对应注释即可。建议继续保留
`workflow_dispatch`，便于人工重试或只在确认内容后发布。

PR 阶段可以后续增加独立的 build-only workflow，只执行 `pnpm install --frozen-lockfile`、typecheck 和 build，
不授予 `pages: write`。
