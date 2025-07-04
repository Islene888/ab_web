import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
from scipy.stats import ttest_ind, norm, mannwhitneyu

# 1. 读取数据
df = pd.read_csv('AOV_user.csv')
print("列名：", df.columns.tolist())
print(df.head())

# 2. 数据预处理
# 确保关键字段都是数值型
df['user_aov'] = pd.to_numeric(df['user_aov'], errors='coerce')
df['variation_id'] = pd.to_numeric(df['variation_id'], errors='coerce')

# 去除user_aov的空值
df = df.dropna(subset=['user_aov'])

# 3. 提取分组
a = df[df['variation_id'] == 0]['user_aov'].values
b = df[df['variation_id'] == 1]['user_aov'].values

print("样本量：对照组", len(a), "实验组", len(b))
print("对照组均值", a.mean(), "实验组均值", b.mean())

# 4. 线性回归
df['intercept'] = 1
X = df[['variation_id', 'intercept']]
y = df['user_aov']
model = sm.OLS(y, X).fit()
print('\n【线性回归】')
print(model.summary())

# 5. t检验
t_stat, t_p = ttest_ind(a, b, equal_var=False)
print('\n【t检验】')
print(f't值={t_stat:.4f}, p值={t_p:.4f}')

# 6. z检验（大样本时可用）
mean_diff = b.mean() - a.mean()
se = np.sqrt(np.var(a, ddof=1)/len(a) + np.var(b, ddof=1)/len(b))
z = mean_diff / se
p_z = 2 * (1 - norm.cdf(abs(z)))
print('\n【z检验】')
print(f'z值={z:.4f}, p值={p_z:.4f}')

# 7. 贝叶斯AB检验（正态分布假设下）
# 7. 贝叶斯AB检验（正态分布假设下，返回uplift均值、置信区间和P(B>A)）
np.random.seed(0)
N = 10000
a_post = np.random.normal(a.mean(), a.std(ddof=1)/np.sqrt(len(a)), N)
b_post = np.random.normal(b.mean(), b.std(ddof=1)/np.sqrt(len(b)), N)
uplift_post = b_post - a_post

# uplift均值
uplift_mean = uplift_post.mean()

# 置信区间（95%）
ci_low, ci_high = np.percentile(uplift_post, [2.5, 97.5])

# 贝叶斯概率 P(B > A)
bayes_prob = (uplift_post > 0).mean()

print('\n【贝叶斯AB检验】')
print(f'P(实验组AOV高于对照组) ≈ {bayes_prob:.3f}')
print(f'Bayesian uplift均值: {uplift_mean:.4f}')
print(f'Bayesian uplift 95%置信区间: [{ci_low:.4f}, {ci_high:.4f}]')


# 8. Bootstrap置信区间和概率
boot_diffs = []
for _ in range(10000):
    a_sample = np.random.choice(a, size=len(a), replace=True)
    b_sample = np.random.choice(b, size=len(b), replace=True)
    boot_diffs.append(b_sample.mean() - a_sample.mean())
boot_diffs = np.array(boot_diffs)
ci_low, ci_high = np.percentile(boot_diffs, [2.5, 97.5])
boot_prob = (boot_diffs > 0).mean()
print('\n【Bootstrap】')
print(f'均值差95%置信区间: [{ci_low:.3f}, {ci_high:.3f}]')
print(f'B>A的概率: {boot_prob:.3f}')

# 9. Mann-Whitney U 检验
u_stat, u_p = mannwhitneyu(a, b, alternative='two-sided')
print('\n【Mann-Whitney U 检验】')
print(f'U值={u_stat:.2f}, p值={u_p:.4f}')

# 10. 可视化（如需，可取消注释）
# plt.figure(figsize=(7,5))
# plt.boxplot([a, b], labels=['对照组', '实验组'])
# plt.title('AOV分布对比')
# plt.ylabel('AOV')
# plt.show()
