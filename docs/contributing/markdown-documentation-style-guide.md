Markdown Documentation Style Guide
==================================

Markdown is great for writing documentation. GitHub natively displays it in a very pleasing, consistent way, it's
easily rendered into a number of other formats, and it's easy to read when it's not rendered (ie. as text). As with
anything that supports rich formatting, however, the best format is not always clear, and understanding documentation is
particularly hard when there isn't a clear, consistent style used throughout a set of documentation. 

To make sure the Fili documentation is all on the same page (not literally...), here are some general guidelines for
Markdown formatting:

Use Block-Level Formatting Judiciously
--------------------------------------

- Headers give an outline of the content in a document, and provide guideposts to readers. Use them!
  - H1 and H2 headers should be defined by underlines (`===` and `---` respectively) rather than by preceding hashes 
    (`#`).
  - For headers defined by underlines, the underlines should have the same number of characters as the text of the 
    header.
  - For headers defined by preceding hashes, trailing hashes are optional, but the number of trailing hashes must match
    the preceding hashes if present.
- If quoting from some other source, use a blockquote (`>` at the start of the line).
- A code sample (in a code block) is often the best way to illustrate something or show an example. Use them liberally!
- Lists are great for breaking up information into small, easy to digest chunks.
  - Use hanging indents for multi-line list elements.
  - "Header-like" lists can use **bolding** to turn the main list elements into "pseudo-headers" if needed. In general, 
    it's better to use actual headers, but they always don't work well (especially if they would be very long).
  - "Definition List"-style lists can use **bolding** to offset the term or phrase from the definition.

Make Documents Interactive by Linking to Other Things
-----------------------------------------------------

- Link to other places in the document (headers or sections, for example) or other documents or resources.
- Include a Table of Contents section for long or complex documents.
- Linking to tests or actual code within the codebase is a great way to show an example of something.
- Don't rely on automatic linking of URLs. Use the documentation text to give the link a more descriptive name.
- Links should use the reference-style:

  ```
  [link text][link reference]
  [example link text][example link]
  
  [link reference]: <actual url>
  [example link]: http://example.com
  ```
    
  rather than the in-line style:
  
  ```
  [example](http://example.com)
  ```
  
- Link references should be organized at the bottom of the document by link reference in alphabetical order with a 
  blank line between each letter of the alphabet:

  ```
  [an example link]: http://example.com
  [another example link]: http://example.com
 
  [be the example link you were meant to be]: http://example.com
 
  [example link]: http://example.com
  ```
  
- In-line links may be used when linking to other markdown documents in the same location. For example:
  
  ```
  [example link](sibling-document)
  ```

- References should be specified explicitly. The following all mean the same in GitHub flavored Markdown:

  ```
  [example link][example link] <-- Preferred
  [example link] 
  [example link][]
  
  [example link]: http://example.com
  ```
  
  However the first (specifying the reference explicitly) decouples the link text from the reference name, and thus is 
  more resilient to change.
    

Use Emphasis Consistently to Call Out Important Aspects
-------------------------------------------------------

- Named code-like entities, (tokens, in the parlance of programming languages) should be emphasized with in-line code
  using backticks (`\``). Class names are a great example of code-like entities that should be emphasized with 
  backticks.
- Emphasized phrases should be _italicized_ using single-underscores (`_`).
- Very important information, like warnings or cautions, should be **bolded** using double-asterisks (`**`).
- Extremely important information, like potential data loss or or other critical information, should be ***bolded and 
  italicized*** using triple-asterisks (`***`).

Use Blank Lines Judiciously
---------------------------

- Headers should have 1 blank line after them, before the content of that section starts.
- Headers should have 1 blank line before them if preceded by other content. If it makes the text much more readable,
  there can be 2 blank lines above a header, but not more than that.
