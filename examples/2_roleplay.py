import batchfactory as bf
from batchfactory.op import *
import batchfactory.op.functional as F

FORMAT_REQ = "Please only output the dialogue."
model = "gpt-4o-mini@openai"

with bf.ProjectFolder("roleplay", 1, 0, 5) as project:
    ###### Setup topics ######
    g = ReadMarkdownLines("./demo_data/greek_mythology_stories.md") | TakeFirstN(1)

    # START_EXAMPLE_EXPORT
    ###### Create the characters and their settings ######
    Teacher = AICharacter("Teacher", "You are a teacher. "+FORMAT_REQ, model=model)
    Student = AICharacter("Student", "You are a student. "+FORMAT_REQ, model=model)

    ###### Introduction of the Role Playing Session ######
    g |= Teacher("Please introduce the text from {headings} titled {keyword}.")
    ###### Main Role Playing Loop ######
    loop_body = Student("Please ask questions or respond.")
    loop_body |= Teacher("Please respond to the student or continue explaining.")
    g |= Repeat(loop_body, 3)
    ###### Wrap Up ######
    g |= Teacher("Please summarize.")
    g |= ChatHistoryToText(template="**{role}**: {content}\n\n")

    ###### Export the Role Playing Session ######
    g |= MapField(lambda headings,keyword: headings+[keyword], ["headings", "keyword"], "headings")
    g |= WriteMarkdownEntries(project["out/roleplay.md"])
    # END_EXAMPLE_EXPORT

g.execute(dispatch_brokers=True)