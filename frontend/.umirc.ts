import { defineConfig } from '@umijs/max';

export default defineConfig({
  antd: {},
  access: {},
  model: {},
  initialState: {},
  request: {},
  layout: {
    title: '@umijs/max',
  },
  routes: [
    {
      path: '/',
      redirect: '/local_msg',
    },
    {
      name: '本地消息',
      path: '/local_msg',
      component: './LocalMsg',
    },
  ],
  npmClient: 'yarn',
});

