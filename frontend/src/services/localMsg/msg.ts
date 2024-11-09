import { post } from '@/utils/axios';

export function msgList(biz: string, db: string, q: {limit:number, offset:number, status: number}) {
  return post('/local_msg/list', {
    biz: biz,
    db:db,
    query: q
  })
}

export function retrySendMsg(biz: string, db: string, table: string, id: number) {
  return post('/local_msg/retry', {
    db: db,
    table: table,
    id: id
  })
}