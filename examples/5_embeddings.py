import batchfactory as bf
from batchfactory.op import *
import numpy as np

with bf.ProjectFolder("embeddings", 1, 0, 0) as project:
    g = bf.Graph()
    g |= ReadMarkdownLines("./demo_data/greek_mythology_stories.md")
    # START_EXAMPLE_EXPORT
    g |= EmbedText("keyword", model="text-embedding-3-small@openai", output_format="list")
    # END_EXAMPLE_EXPORT
    g |= (out:= ToList("embedding","keyword"))

g.execute(dispatch_brokers=True)

result = out.get_output()

similarities = []
for i in range(len(result)):
    for j in range(i + 1, len(result)):
        embedding1 = np.array(result[i]["embedding"])
        embedding2 = np.array(result[j]["embedding"])
        similarity = np.dot(embedding1, embedding2) / (np.linalg.norm(embedding1) * np.linalg.norm(embedding2))
        similarities.append((result[i]["keyword"], result[j]["keyword"], similarity))

similarities.sort(key=lambda x: x[2], reverse=True)

with open(project["out/similarities.txt"], "w") as f:
    for keyword1, keyword2, similarity in similarities:
        f.write(f"Similarity between '{keyword1}' and '{keyword2}': {similarity:.4f}\n")
    

