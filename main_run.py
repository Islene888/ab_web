import warnings
from symbol import subscript

from urllib3.exceptions import NotOpenSSLWarning

from state3.Advertisement import advertisement, advertisement_sum
from state3.Business import Main_business
from state3.Engagement import Main_Engagement
from state3.Recharge import recharge, recharge_summury
from state3.Retention import retention_report_table_ETL, Main_Retention
from state3.Retention.retention_wide_table_ETL import insert_experiment_data_to_wide_table
from state3.Subscribe import subscribe, subscribe_sum
from state3.chat_click_show import Main_Chat_click_show
from state3.growthbook_fetcher.experiment_tag_all_parameters import get_experiment_details_by_tag
from state3.growthbook_fetcher.growthbook_data_ETL import fetch_and_save_experiment_data

warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
import warnings
from sqlalchemy.exc import SAWarning
warnings.filterwarnings("ignore", category=SAWarning)

# 获取并保存 GrowthBook 实验数据
fetch_and_save_experiment_data()

# 定义实验标签
tag = 'chat_0519'


# 1.留存计算
Main_Retention.main(tag)

# 2.聊天点击
Main_Chat_click_show.main(tag)

# 3.engagement 互动功能
Main_Engagement.main(tag)

# 4.business 商业化
Main_business.main(tag)

# 5.充值计算
recharge.main(tag)
recharge_summury.main(tag)

# 6.subscribe 订阅
subscribe.main(tag)
subscribe_sum.main(tag)


# 7.advertisement.main(tag)
# advertisement_sum.main(tag)














