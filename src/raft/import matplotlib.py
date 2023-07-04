import matplotlib.pyplot as plt
import matplotlib.animation as animation

# 模拟日志序列
log_sequence = [
    ("send_request_vote", 1, 2),
    ("send_request_vote", 3, 1),
    ("grant_vote", 2, 1),
    ("grant_vote", 3, 1),
    ("reject_vote", 2, 3),
    ("send_request_vote", 2, 3),
    ("grant_vote", 3, 2),
    ("send_heartbeat", 2),
    ("send_heartbeat", 3)
]

# 创建动画图表
fig, ax = plt.subplots()
ax.set_xlim(0, len(log_sequence))
ax.set_ylim(0, 5)
ax.set_xticks(range(len(log_sequence)))
ax.set_xticklabels([f"Log {i+1}" for i in range(len(log_sequence))])

# 定义用于更新动画的函数
def update_animation(frame):
    log_type, node_i, node_d = log_sequence[frame]
    if log_type == "send_request_vote":
        ax.annotate(f"Node {node_i} sends\nrequest vote to\nNode {node_d}", (frame, 3), ha='center')
        ax.plot(frame, 3, 'bo')
    elif log_type == "grant_vote":
        ax.annotate(f"Node {node_i} grants\nvote to Node {node_d}", (frame, 2), ha='center')
        ax.plot(frame, 2, 'go')
    elif log_type == "reject_vote":
        ax.annotate(f"Node {node_i} rejects\nvote to Node {node_d}", (frame, 1), ha='center')
        ax.plot(frame, 1, 'ro')
    elif log_type == "send_heartbeat":
        ax.annotate(f"Leader sends\nheartbeat to\nNode {node_i}", (frame, 4), ha='center')
        ax.plot(frame, 4, 'yo')

# 创建动画对象
ani = animation.FuncAnimation(fig, update_animation, frames=len(log_sequence), repeat=False)

# 显示动画
plt.show()
