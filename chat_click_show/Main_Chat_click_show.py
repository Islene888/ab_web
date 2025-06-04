import time
from state3.chat_click_show.active import  Chat_active, click_ratio, bot_chat_start_ratio, Time_spent
from state3.chat_click_show.explore import explorec_hat_click_show, avg_show, Chat_explore, click_chat_ratio, \
    home_insert


def run_event(event_name, event_func, tag):
    print(f"\n🚀 开始执行 {event_name} 事件，标签：{tag}")
    start_time = time.time()
    try:
        event_func(tag)
        print(f"✅ {event_name} 事件执行完成，耗时：{round(time.time() - start_time, 2)}秒")
    except Exception as e:
        print(f"❌ {event_name} 事件执行失败，错误信息：{e}")


def main(tag):
    print(f"\n🎬 【主流程启动】标签：{tag}\n")

    events = [
        ("Chat_explore", Chat_explore.main),
        ("click_ratio", click_ratio.main),
        ("Time_spent", Time_spent.main),
        ("avg_show", avg_show.main),
        ("Chat", Chat_active.main),
        ("bot_chat_start_ratio", bot_chat_start_ratio.main),
        ("click_chat_ratio", click_chat_ratio.main),
        ("home_insert", home_insert.main),
        ("explorec_hat_click_show", explorec_hat_click_show.main)

    ]

    for event_name, event_func in events:
        run_event(event_name, event_func, tag)

    print("\n🎉 【所有事件处理完毕】")


if __name__ == "__main__":
    tag = "chat_0508"  # 未来可以从外部传入或读取配置
    main(tag)
