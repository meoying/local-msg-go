import React, { useEffect, useState } from 'react';
import {
  ProForm,
  ProFormDateRangePicker,
  ProFormDateTimeRangePicker, ProFormGroup, ProFormSelect,
  ProFormText,
  ProTable,
} from '@ant-design/pro-components';
import { formatLocaleTime } from '@/utils/format';
import { msgList, retrySendMsg } from '@/services/localMsg/msg';
import { message } from 'antd';

interface Props {

}

const statusOptions = [
  {
    label: "全部",
    value: -1,
  },
  {
    label: "初始化",
    value: 0,
  },
  {
    label: "成功",
    value: 1,
  },
  {
    label: "失败",
    value: 2,
  },
]

const retryMsg = (biz: string, db: string, table: string, id: number) => {
  retrySendMsg(biz, db, table, id).then(res => {
    // const code = res?.data?.code || -1
    message.success("重试成功!")
  })
}

const columns = [
  {
    title: 'Biz',
    dataIndex: 'biz',
    hideInTable: true,
    renderFormItem: () => {
      return <ProFormText name="biz" placeholder={'业务'}/>
    }
  },
  {
    title: 'DB',
    dataIndex: 'db',
    hideInTable: true,
    renderFormItem: () => {
      return <ProFormText colProps={{ md: 12, xl: 8 }} placeholder={'分库了可以填'}/>
    }
  },
  {
    title: 'Table',
    dataIndex: 'table',
    hideInTable: true,
    renderFormItem: () => {
      return <ProFormText colProps={{ md: 12, xl: 8 }} placeholder={'分表了可以填'}/>
    }
  },
  {
    title: 'ID',
    dataIndex: 'id',
    search: false,
  },
  {
    title: 'Key',
    dataIndex: 'key',
    copiable: true
  },
  {
    title: 'Topic',
    dataIndex: ['msg', 'topic'],
    search: false,
  },
  {
    title: 'Partition',
    dataIndex: ['msg', 'partition'],
    search: false,
  },
  {
    title:'消息内容',
    dataIndex: ['msg', 'content'],
    search: false,
  },
  {
    title: '状态',
    dataIndex: 'status',
    render: (text, record) => {
      const status = record?.status || 0;
      switch (status) {
        case 0:
          return "初始化"
        case 1:
          return "成功"
        case 2:
          return "失败"
        default:
          return "未知状态"
      }
    },
    renderFormItem: () => {
      return <ProFormSelect name="status"
                            options={statusOptions}/>
    }
  },
  {
    title: '创建时间',
    dataIndex: 'ctime',
    render: (text, record) => {
      return formatLocaleTime(record?.ctime || 0)
    },
    colSize: 4,
    renderFormItem: () => {
      return <ProFormDateTimeRangePicker
        name="dateRange"
      />
    }
  },
  {
    title: '更新时间',
    dataIndex: 'utime',
    render: (text, record) => {
      return formatLocaleTime(record?.utime || 0)
    },
    search: false,
  },
  {
    title: '发送次数',
    dataIndex: 'sendTimes',
    search: false,
  },
  {
    title: '操作',
    dataIndex: 'option',
    valueType: 'option',
    search: false,
    render: (_, record) => (
      <>
        <a
          onClick={() => {
            retryMsg(record?.biz, record?.db, record?.table, record?.id)
          }}
        >
          重试
        </a>
      </>
    ),
  }
]

const Page: React.FC<Props> = (props) => {
  // const [msgs, setMsgs] = useState<LocalMsg[]>([]);

  const buildQuery = (vals) => {
    const range = vals?.ctime
    if(range) {
      const startTime = range[0]
      const endTime = range[1]
      vals.startTime = new Date(startTime).getTime();
      vals.endTime = new Date(endTime).getTime() ;
    }
    return vals
  }

  const loaddata = async (params, sorter, filter) => {
    const query = buildQuery(params)
    query.offset = (params.current - 1) * params.pageSize
    query.limit = params.pageSize
    query.status = query?.status || -1
    const res = await msgList(query.biz, query.db, query);
    const data = res?.data?.data || []

    return {
      data: data,
      success: true
    }
  }

  return <div>
    <ProTable columns={columns}
              rowKey={'id'}
              // dataSource={msgs}
              search={{collapsed: false}}
              request={loaddata}
    >
    </ProTable>
  </div>
};

export default Page;