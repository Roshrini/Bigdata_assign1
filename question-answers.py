__author__ = 'roshaninagmote'

def parse_story(story_filename):
     story_dict = {}
     with open(story_filename) as myfile:
         parts = myfile.read().split("TEXT:")

         headline = parts[0].splitlines()[0]
         date = parts[0].splitlines()[1]
         storyid = parts[0].splitlines()[2]

         text = parts[1].strip("\n").replace("\n"," ").split(".")
         story_dict[(headline,date,storyid)] = text
     return story_dict


def data_forward(questions_data,story_dict):
    for question in questions_data:
        for story_key in story_dict:
            text_list = story_dict[story_key]
            print(question[1],text_list)
            wordmatch(question[1],text_list)


if __name__ == "__main__":
    input_path = "/Users/roshaninagmote/Downloads/developset/"
    input_file = open(input_path+"input.txt")
    input_data = input_file.read().splitlines()
    path = input_data[0]
    for i in range(1,len(input_data)):

        each_story = input_data[i]+".story"
        each_question = input_data[i]+".questions"
        story_file = open(input_path+each_story)
        questions_file = open(input_path+each_question)
        story_data = story_file.read()
        questions_data_raw = questions_file.read().splitlines()
        questions_total = filter(None, questions_data_raw)

        que = questions_file.read()

        questions_data = []
        for j in range(0,len(questions_total),3):
            question_temp = []
            quesid = questions_total[j].split(":")[1].lstrip(" ")
            question_temp.append(quesid)
            ques = questions_total[j+1].split(":")[1].lstrip(" ")
            question_temp.append(ques)
            question_temp.append(questions_total[j+2])

            questions_data.append(question_temp)


        story_dict = parse_story(input_path+each_story)

        data_forward(questions_data,story_dict)
