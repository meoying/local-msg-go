import axios, {  AxiosRequestConfig,} from 'axios';
import { message } from 'antd';

const instance = axios.create({
  // 这边记得修改你对应的配置文件
  baseURL: "http://localhost:8080",
  withCredentials: true,
});

export interface Result<T> {
  code: number;
  msg: string;
  data: T;
}

export function post<T>(url: string, data?: any, cfg?: AxiosRequestConfig) {
  return instance.post<Result<T>>(url, data, cfg);
}

// 响应中要处理一些响应头部
instance.interceptors.response.use(
  (resp) => {
    return resp;
  },
  (err) => {
    const status = err?.response?.status || 500;
    message.error('系统错误: ' + status, 3);
    console.log(err);
    return Promise.reject(err)
  },
);