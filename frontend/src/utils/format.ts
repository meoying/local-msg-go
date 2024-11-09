import dateFormat from 'dateformat';

// 格式化为本地时间表达
export function formatLocaleTime(ms: number) {
  const date = new Date(ms || 0);
  return dateFormat(date, 'yyyy-mm-dd HH:MM:ss');
}
