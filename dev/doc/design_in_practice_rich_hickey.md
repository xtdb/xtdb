# Design in practice

[https://www.youtube.com/watch?v=fTtnx1AAJ-c](https://www.youtube.com/watch?v=fTtnx1AAJ-c)

- It’s about writing out your thoughts. Having the problem right in your face all the time.
- Writing as part of thinking.
- Add a Glossary to your project
- Asking the right questions is a good skill.
    - Where are you at ?
    - Where are you going ?
    - What do you know ?
    - What do you need to know?
- The Socratic method

### Reflective inquiry

|  | Understanding | Activity |
| --- | --- | --- |
| Status - ‘to stand’ | What do you know? | Where are you at? |
| Agenda - ‘to be done’ | What do you need to know? | Where are you going? |

### Design Phase

- non monotonic
- backtracking - after you have learned something, you might retract and go to an earlier phase
- Deciding: Scoping is always possible. Rejecting is always possible.

### A Story (the shaping of it)

- Describe
    - What do you know? - something is off/missing
    - What do you need to know? - the extent of it
    - Where are you at? - observing, listening
    - Where are you going?
        - initial title of the story
        - write down a description in the top story
- Description
    - 1 summary paragraph
    - situation you find yourself in
    - not the reasons for the problem or possible causes
- Diagnose
    - bug/feature - both of them might need a different approach
        - feature
            - “We don’t have feature X!” is not a valid Problem statement.
            - Go from feature → problem
                - The feature is then 1 possible answer.
                - Open set of solutions (this is where design happens).
    - What do you know? - the symptoms/context
    - What do you need to know? - the cause
    - Where are you at? - good description (one above), evidence
    - Ware are you going? - applying logic and experimentation
- Delimit
    - the problem you are going to solve next as multiple problems might have appeared during the diagnose phase
    - What do you know ? - the problem
    - What do yo need to know ? - How to state it succinctly. Its scope.
    - Where are you at ? - having done the diagnosis (one above)
    - Where are you going ? - Writing the problem statement.
        - Succinct statement of unmet user objectives and causes
        - Story title might change
- Direction
    - Where are you at? - Description and Problem statement
    - Where are you going? - The approaches
        - Enumerating use cases
            - Not how, but intentions and objectives (of the user
        - Making a strategy DM (Decision Matrix)
            - A1 - Problem statement
            - First column - criteria
            - Other columns - approaches
            - Cells should be descriptive - not just yes/no, good/bad etc.
            - Avoid subjective statements
            - Subjectivity is ONLY added via colors
        - Determining the scope
        - It’s not shopping (just find different approaches). It’s about going through the exercise of examining how the approaches differ from each other (via the criteria)
- Design
    - You can’t start here. All of the above have to come first.
    - Where are you at? use case and strategy DM (from above)
    - What do you know ? - The problem and the direction we are going to take (previous phases)
    - What do you need to know ?
        - implementation approaches
        - the best of these
        - What matters in deciding.
- Dev - only now