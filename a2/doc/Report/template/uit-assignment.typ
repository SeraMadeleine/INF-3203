
#let conf(
  faculty: str,
  title: str,
  subtitle: str,
  name: str,
  email: str,
  course: str,
  semester: str,
  year: str,
  doc,
) = {

set document(
  title: course + ", " + title,
  author: name + " <" + email + ">",
  date: auto
)

set page(
  paper: "a4",
  header: place(left + top, image("uit-logo.png", alt: "UiT Logo", width: 20cm), dx: -2cm, dy: 0.5cm),
)

  // place(top + left,, clearance: 0cm)
  // image("uit-logo.png", alt: "UiT Logo")
  linebreak()
  linebreak()
  linebreak()
  linebreak()
  set align(left)
  text(11pt, faculty, )
  linebreak()
  linebreak()
  text(14pt, title, weight: "bold")
  linebreak()
  text(12pt, subtitle)
  linebreak()
  linebreak()
  text(10pt, name + ", " + link("mailto:"+email))
  linebreak()
  text(10pt, course + ", " + semester + " " + year)

  place(left + bottom, image("uit-footer.png", alt: "UiT footer", width: 21.2cm), dx: -2.5cm, dy: 3.7cm)

  pagebreak()
  set page(paper: "a4", header: [])
  set align(left)
  columns(1, doc)

}
