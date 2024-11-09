export interface LocalMsg {
  id: number,
  msg: {
    topic: string
    partition: number,
    content: string,
  },
  status: number
  utime: number
  ctime: number
}