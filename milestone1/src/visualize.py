import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt

df = pd.read_csv("data/final/merged_disease_symptom_list.csv")
text = " ".join(df["treatments"].dropna().astype(str))

wordcloud = WordCloud(
    width=600,
    height=600,
    background_color="white",
    colormap="viridis",
    max_words=150
).generate(text)

plt.figure(figsize=(12, 8))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.title("Word Cloud of Treatments", fontsize=15, pad=30)
plt.show()

wordcloud.to_file("data/final/plots/treatments_wordcloud.png")
print("Word cloud saved as 'treatments_wordcloud.png' in 'data/final/plots' directory.")
 