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

当前 `.github/workflows/docs-pages.yml` 把构建和部署拆为两个 Job：build 安装锁定依赖、执行 typecheck 和
Docusaurus build，再上传 `website/build`；deploy 依赖 build，并通过 `github-pages` environment 发布。
这与 [GitHub Pages 自定义 Workflow 的官方流程](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
一致。

### 首次发布检查

1. 确认仓库已启用 Actions，并允许使用仓库中的 Actions。
2. Settings → Pages 的 Source 选择 GitHub Actions。
3. Actions 中手动运行 workflow，分支选择包含文档最新提交的分支。
4. build Job 成功后检查 artifact 上传步骤，再检查 deploy Job。
5. 在 deployment 输出中打开实际 `page_url`，不要根据仓库名猜地址。
6. 首次 DNS/CDN 缓存生效后，再验证刷新和直接访问二级页面。

## 启用自动发布

workflow 中保留了被注释的 `push: branches: [master]`。需要自动发布时取消对应注释即可。建议继续保留
`workflow_dispatch`，便于人工重试或只在确认内容后发布。

PR 阶段可以后续增加独立的 build-only workflow，只执行 `pnpm install --frozen-lockfile`、typecheck 和 build，
不授予 `pages: write`。

## 常见故障

| 问题 | 检查项 |
| --- | --- |
| Actions 页面没有 Run workflow | workflow 是否在 GitHub 已存在的分支上，且包含 `workflow_dispatch`。 |
| build 找不到依赖 | Node/pnpm 版本、`website/pnpm-lock.yaml` 和 frozen lockfile 是否一致。 |
| 首页正常、资源 404 | `url`、`baseUrl` 与项目 Pages 路径是否匹配。 |
| 二级页面刷新 404 | artifact 是否完整，链接是否使用 Docusaurus base URL。 |
| deploy 权限失败 | workflow 是否有 `pages: write` 和 `id-token: write`。 |
| 自定义域名仍跳旧地址 | GitHub Pages 域名设置、DNS 和 Docusaurus `url/baseUrl` 是否同时更新。 |

GitHub Pages 的自定义 workflow 需要上传 Pages artifact，并由 `deploy-pages` 在 `github-pages` environment
中部署。失败时先查看对应 workflow run 的 build/deploy 分界，不要通过提交 `website/build/` 绕过问题。
